# DE-07-homework

## lesson_02

### Running the jobs
Stay in the root folder of this repository.  Specify it as PYTHONPATH env variable so that the package `lesson_02` 
can be found.    

To launch the jobs in background run: 
```sh
PYTHONPATH=. python lesson_02/job1/main.py &
PYTHONPATH=. python lesson_02/job2/main.py &
```
Alternatively, run in separate terminals, to keep the jobs in the foreground: 

```sh
PYTHONPATH=. API_AUTH_TOKEN=<auth token> python lesson_02/job1/main.py 
```

```sh
PYTHONPATH=. python lesson_02/job2/main.py 
```

### Checking the jobs
After the jobs are running to check them run:
```sh
python lesson_02/bin/check_jobs.py 
```

### Tests

```sh
cd lesson_02/job1/tests
PYTHONPATH=../../../ python -m unittest
cd - 
cd lesson_02/job2/tests
PYTHONPATH=../../../ python -m unittest
```