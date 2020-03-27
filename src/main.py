
import sys
import os
import csv
import time
import requests
import traceback
from concurrent import futures
import queue
import functools
import threading

from keboola import docker as kbc_docker

INPUT_TOKEN = 1
MESSAGE_ERROR = 'ERROR'
MESSAGE_OK = 'OK'
MESSAGE_COMMUNICATION_OVER = 'OUT'
EMPTY_QUEUE_INDICATOR = '__EMPTY__'


def data_producer(input_file_path: str, max_entries_per_second: int):
    """
    A generator that produces a single string for each row of a CSV file. If the file has more than one column,
    a ValueError is raised. This function also contains a limiter that keeps the rate of returned data under
    a certain number per second.
    :param input_file_path: A path to the CSV file.
    :param max_entries_per_second: The maximum number of rows returned per second by this generator.
    """
    limit = max_entries_per_second
    limit_t = time.monotonic() - 1

    with open(input_file_path, 'r', encoding='utf-8-sig') as input_file:
        input_reader = csv.DictReader(input_file)

        while True:
            if time.monotonic() - limit_t >= 1:
                limit = max_entries_per_second
                limit_t = time.monotonic()

            if limit > 0:
                try:
                    entry_ = next(input_reader)

                    row_data = list(entry_.values())
                    if len(row_data) > 1:
                        print(
                            'The input table must have only one column, {} detected. Offending row: {}'.
                            format(len(row_data), row_data),
                            file=sys.stderr
                        )
                        raise ValueError

                    yield row_data[0]
                    limit -= 1
                except StopIteration:
                    break
            else:
                time.sleep(0.01)


def request_callback(result_queue: queue.Queue, request_future: futures.Future):
    """
    A callback to check the result of a single call to the target API and passes the message about its success/failure
    to the result queue to be collected downstream. If the future ended in an exception, or if the status code
    of the API call result is not 2xx, the function logs an exception and passes an error indicator
    into the result queue. Otherwise it just passes an OK message into the result queue.
    :param result_queue: A queue that collects success/failure messages from the request callbacks.
    :param request_future: A future representing a call to the target api.
    """
    result = None
    try:
        exc = request_future.exception()
        if exc:
            print(exc, file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
        else:
            res = request_future.result()
            if res.status_code // 100 == 2:
                # if the execution ended up here, everything is ok
                result = MESSAGE_OK
            else:
                print('Request failed. Status code: {}, text: {}'.format(res.status_code, res.text), file=sys.stderr)
    except Exception as exc:
        print(exc, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

    # if result is None, something must have gone wrong and send error message; otherwise send the OK message
    result_queue.put(result or MESSAGE_ERROR)


def result_checker(input_queue: queue.Queue, partial_results_queue: queue.Queue,
                   logging_item_batch_size: int,
                   final_result_queue: queue.Queue):
    """
    The task of this function is to collect messages about input reads and api call results, regularly log progress
    and detect the completion of the whole read-send process. After receiving the message about the input reading
    being completed, it waits for the number of messages in the partial result queue to be equal to the number
    of messages in the input queue. When that happens, the function sends a single message to the final result queue
    to indicate that all inputs have been sent to the api and terminates. If it received any message about an error
    in the partial result queue, the message sent to the final result queue is an error indicator.

    :param input_queue: A queue containing a single item for each input row. When all the inputs have been collected,
    the producer must send an object indicating that no more data is coming.
    :param partial_results_queue: A queue containing single item for each response from the target api. Each item is
    either a success or failure indicator. No communication ending indicator is expected in this queue (because
    the api calls are processed asynchronously from many producers, non of which knows that all
    the data has been processed)
    :param logging_item_batch_size: We do not want to log progress after each input/api response, so this number
    is the number of inputs/api responses must be received to trigger a progress log.
    :param final_result_queue: A queue to which this function sends a single message when it receives api response
    messages for all the input messages to indicate that the whole process is over.
    """

    def _get_from_queue(qu: queue.Queue):
        try:
            return qu.get_nowait()
        except queue.Empty:
            return EMPTY_QUEUE_INDICATOR

    input_count = 0
    input_count_final = False
    result_count = 0
    error_result_count = 0

    # we run this loop until we have received result messages for all inputs
    while not (input_count_final and result_count >= input_count):
        # check if there are any new data in any of the queues
        input_message = _get_from_queue(input_queue)
        partial_res_message = _get_from_queue(partial_results_queue)

        if input_message == partial_res_message == EMPTY_QUEUE_INDICATOR:
            # both queues are empty - wait for something to appear
            time.sleep(0.01)
            continue

        if input_message != EMPTY_QUEUE_INDICATOR:
            # input queue contained new data

            if input_message == MESSAGE_COMMUNICATION_OVER:
                # all inputs have been sent and no more are coming, the input count is now final
                input_count_final = True

                if input_count % logging_item_batch_size == 0:
                    print('Finished reading the input table. {} items read in total'.format(input_count))
            else:
                # the queue contained an indicator of another input
                input_count += 1

                if input_count % logging_item_batch_size == 0:
                    print('{} items read from the input table'.format(input_count))

        if partial_res_message != EMPTY_QUEUE_INDICATOR:
            # the queue with results from api requests contained new data

            result_count += 1
            if partial_res_message == MESSAGE_ERROR:
                error_result_count += 1

            if result_count % logging_item_batch_size == 0:
                print(
                    '{} items sent to api. {} errors, {} successes'.
                    format(input_count, error_result_count, input_count - error_result_count)
                )

    # when everything is done, we put one message into the final queue to indicate that the processing is finished
    # and to inform the main thread whether any errors occurred
    finished_log_msg = \
        'Finished sending data to api. {} errors, {} successes'.\
        format(error_result_count, input_count - error_result_count)
    if error_result_count:
        # something failed, log result as error and send error message to the final result queue
        print(finished_log_msg, file=sys.stderr)
        final_result_queue.put(MESSAGE_ERROR)
    else:
        # processed finished ok, log the result as info and send an ok message to the final result queue
        print(finished_log_msg)
        final_result_queue.put(MESSAGE_OK)


def main():
    data_dir = os.environ.get('KBC_DATADIR', '/data/')

    cfg = kbc_docker.Config(data_dir=data_dir)
    cfg_intabs = cfg.get_input_tables()
    cfg_pars = cfg.get_parameters()

    max_requests_per_second = cfg_pars['max_requests_per_second']
    auth_token = cfg_pars['#bearer_token']
    logging_item_batch_size = cfg_pars['logging_item_batch_size']

    print('Loading configuration loaded.')

    if len(cfg_intabs) != 1:
        print(
            "Exactly one input table expected! {} are present".format(len(cfg_intabs)),
            file=sys.stderr
        )
        sys.exit(1)

    input_table_path = cfg_intabs[0]['full_path']

    input_notification_queue = queue.Queue()
    result_notification_queue = queue.Queue()
    final_result_queue = queue.Queue()

    result_checker_thread = threading.Thread(
            target=result_checker,
            args=(input_notification_queue, result_notification_queue, logging_item_batch_size, final_result_queue)
        )
    result_checker_thread.start()

    with futures.ThreadPoolExecutor(max_workers=max_requests_per_second) as extr:
        for item_ in data_producer(input_table_path, max_requests_per_second):
            ftr = extr.submit(
                    requests.put,
                    url='https://mg-xsell.mallgroup.com/api/v1/x-sell-products/recommended',
                    headers={
                            'Content-Type': 'application/json',
                            'Authorization': 'Bearer {}'.format(auth_token)
                        },
                    data=item_
                )

            ftr.add_done_callback(
                functools.partial(request_callback, result_notification_queue)
            )

            input_notification_queue.put(INPUT_TOKEN)

    input_notification_queue.put(MESSAGE_COMMUNICATION_OVER)

    result_checker_thread.join()
    final_result = final_result_queue.get()
    if final_result == MESSAGE_ERROR:
        sys.exit(1)


if __name__ == '__main__':
    main()
