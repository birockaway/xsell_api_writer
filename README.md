# keboola_component_template

How to setup automatic deployment using this github account:
1) Use this template to create a new public repo.
2) Put your code to src/main.py (or change the path in Dockerfile) and add requirements.
3) Go to travis-ci.org (sign via this github account), click "Settings" and "Sync account". Your new repo should appear.
4) Register component at https://components.keboola.com (This must be done before step 5), but not necessarily right before it. You can do it as the first step if you feel like it).
5) Activate repo in travis (just click it) and set up environmental variables as specified in vault/bi/keboola_deploy_tools

*Done*

With each commit, travis will build your component. If you add release tag to the master branch, your component will be deployed in keboola.
