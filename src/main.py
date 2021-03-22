import sys
import os
import csv
import time
import requests
from concurrent import futures
import queue
import functools
import threading
import logging
from typing import List, Dict, Union, Iterator

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from keboola import docker as kbc_docker
from log import setup_logger

INPUT_TOKEN = 1
MESSAGE_ERROR = 'ERROR'
MESSAGE_OK = 'OK'
MESSAGE_COMMUNICATION_OVER = 'OUT'
EMPTY_QUEUE_INDICATOR = '__EMPTY__'
API_WORKERS_COUNT = 3

setup_logger()
logger = logging.getLogger()


def produce_line(line: Dict[str, str]) -> str:
    line_data = list(line.values())
    if len(line_data) > 1:
        logger.info(
            'The input table must have only one column, %s detected. Offending row: %s',
            len(line_data), line_data,
        )
        raise ValueError

    return line_data[0]


def data_producer(input_file_path: str) -> Iterator[str]:
    """
    A generator that produces a single string for each row of a CSV file. If the file has more than one column,
    a ValueError is raised.
    :param input_file_path: A path to the CSV file.
    """

    with open(input_file_path, 'r', encoding='utf-8-sig') as input_file:
        input_reader = csv.DictReader(input_file)
        for line in input_reader:
            yield produce_line(line)


def request_callback(result_queue: queue.Queue, request_future: futures.Future) -> None:
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
            logger.error(str(exc))
        else:
            res = request_future.result()
            if res.status_code // 100 == 2:
                # if the execution ended up here, everything is ok
                result = MESSAGE_OK
            else:
                logger.error('Request failed. Status code: %s, text: %s', res.status_code, res.text)
    except Exception as exc:
        logger.exception(str(exc))

    # if result is None, something must have gone wrong and send error message; otherwise send the OK message
    result_queue.put(result or MESSAGE_ERROR)


def result_checker(input_queue: queue.Queue, partial_results_queue: queue.Queue,
                   logging_item_batch_size: int,
                   final_result_queue: queue.Queue,
                   allowed_errors_share: float) -> None:
    """
    The task of this function is to collect messages about input reads and api call results, regularly log progress
    and detect the completion of the whole read-send process. After receiving the message about the input reading
    being completed, it waits for the number of messages in the partial result queue to be equal to the number
    of messages in the input queue. When that happens, the function sends a single message to the final result queue
    to indicate that all inputs have been sent to the api and terminates. If it received more than the specified
    share of errors in the partial result queue, the message sent to the final result queue is an error indicator.

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
    :param allowed_errors_share: A parameter specifying what share of unsuccessful requests is acceptable. If the
    share of errors is above this value, the final result is an error indicator.
    """

    def _get_from_queue(qu: queue.Queue) -> str:
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
        input_message = _get_from_queue(input_queue)
        partial_res_message = _get_from_queue(partial_results_queue)

        if input_message == partial_res_message == EMPTY_QUEUE_INDICATOR:
            time.sleep(0.01)  # wait for something to appear
            continue

        if input_message != EMPTY_QUEUE_INDICATOR:
            # input queue contained new data
            if input_message == MESSAGE_COMMUNICATION_OVER:
                # all inputs have been sent and no more are coming, the input count is now final
                input_count_final = True

                if input_count % logging_item_batch_size == 0:
                    logger.info('Finished reading the input table. %s items read in total', input_count)
            else:
                # the queue contained an indicator of another input
                input_count += 1

                if input_count % logging_item_batch_size == 0:
                    logger.info('%s items read from the input table', input_count)

        if partial_res_message != EMPTY_QUEUE_INDICATOR:
            # the queue with results from api requests contained new data

            result_count += 1
            if partial_res_message == MESSAGE_ERROR:
                error_result_count += 1

            if result_count % logging_item_batch_size == 0:
                logger.info(
                    '%s items sent to api. %s errors, %s successes',
                    result_count, error_result_count,
                    result_count - error_result_count
                )

    logger.info(
        'Finished sending data to api. %s errors, %s successes',
        error_result_count, input_count - error_result_count
    )

    if error_result_count / max(input_count, 1) > allowed_errors_share:
        logger.error('The share of errors is higher than allowed_errors_share.')
        final_result_queue.put(MESSAGE_ERROR)
    else:
        # processed finished ok, log the result as info and send an ok message to the final result queue
        final_result_queue.put(MESSAGE_OK)


def main():
    data_dir = os.environ.get('KBC_DATADIR', '/data/')

    cfg: kbc_docker.Config = kbc_docker.Config(data_dir=data_dir)
    cfg_intabs: List[Dict[str, str]] = cfg.get_input_tables()
    cfg_pars: Dict[str, Union[str, int, float]] = cfg.get_parameters()

    api_endpoint: str = cfg_pars.get('api_endpoint')
    auth_token: str = cfg_pars['#bearer_token']
    logging_item_batch_size: int = cfg_pars['logging_item_batch_size']
    allowed_errors_share: float = cfg_pars.get('allowed_errors_share', 0)

    logger.info('Configuration loaded.')

    if len(cfg_intabs) != 1:
        logger.info(
            "Exactly one input table expected! %s are present", cfg_intabs
        )
        sys.exit(1)

    input_table_path = cfg_intabs[0]['full_path']

    input_notification_queue = queue.Queue()
    result_notification_queue = queue.Queue()
    final_result_queue = queue.Queue()

    requests_session = requests.Session()
    with requests_session as session:
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[104]) # retrying on connection reset status
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))

        result_checker_thread = threading.Thread(
            target=result_checker,
            args=(input_notification_queue, result_notification_queue, logging_item_batch_size,
                  final_result_queue, allowed_errors_share)
        )
        result_checker_thread.start()

        with futures.ThreadPoolExecutor(max_workers=API_WORKERS_COUNT) as extr:
            for item_ in data_producer(input_table_path):
                ftr = extr.submit(
                    session.put,
                    url=api_endpoint,
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer {}'.format(auth_token)
                    },
                    data=item_.encode('utf-8')
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
