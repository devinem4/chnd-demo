# chnd-demo
A quick proof of concept of:
1. loading csv files to google storage bucket
2. uploading those csv files as google bigQuery tables

### setup
https://cloud.google.com/python/setup  

I hit an error for `ModuleNotFoundError: No module named '_sqlite3'`. This [unrelated github issue](https://github.com/sloria/TextBlob/issues/173#issuecomment-321291279) pointed me in the right direction.

Also set your env variable for credentials as so:  
`export GOOGLE_APPLICATION_CREDENTIALS="/home/mddevine/projects/chnd-demo/chnd-demo-8f9f88d84914.json"
