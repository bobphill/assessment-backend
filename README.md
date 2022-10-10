# Blue Bite Assessment

## Introduction

This repo includes a stub Django project. We would like you to extend this project by adding three
routes and related validation logic. Included are example files and schema for the expected JSON
request structure.

We use this test to gain insight into your approach when solving a general problem, and to
gauge your familiarity with Django and its related dependencies. So if you are unable to complete
all the requirements due to time constraints or other issues with the local environment, please
submit your partial work. Consider adding comments or a readme with some next steps you would take
if you had more time.

While we are a test-driven engineering team, we understand take-home assessments can be
time-consuming and do not expect unit tests for this project, though if you'd like to write some
by all means feel free!

## Data Constraints

There is a JSON Schema that describes the format of incoming API request bodies.
[It can be found  here](files/schema.json). You may use it to validate the requests.
**Alternative validation methods are allowed**.

Each object record will be part of an array and will look something like this:
```json
{
  "object_id": "0fadf66d045648bfa880d7d07af203bb",
  "data": [
    {
      "key": "type",
      "value": "shoe"
    },
    {
      "key": "color",
      "value": "blue"
    },
    {
      "key": "demo",
      "value": true
    },
    {
      "key": "cost",
      "value": 20.34
    }
  ]
}
```

Example request bodies can be found in [files directory](files)

## Requirements

You are welcome to use whatever amount of time you see fit on this assessment, but we do not expect
developers to spend more than two hours implementing both parts. **If you used more or less than the
two-hour allotment, please indicate how much time you used in your repo's README.**

### Part 1

* The application should accept JSON requests via a route.
    * When the application receives a new request it must be validated, parsed and (if the data is correct) stored in the database.
    * The application must be able to handle validation errors. The format of the errors is your choice so long as it is consistent and adheres to HTTP protocol standards.

### Part 2
* Add two routes for Object retrieval
  * One retrieval endpoint that returns an object for a given `object_id`.
  * One list endpoint that can be filtered based on `data` keys and/or values.

## Submission

After finishing the requirements, or reaching your time constraints, please document any unfinished
work you would have completed if given more time. Feel free to add any additional information you'd
like us to know about your assessment as well (code documentation, decision process for a specific
design choice etc). Once completed please email your point of contact at Blue Bite a link to the
repository.

## Local Setup

This repo includes support for spinning up Django and a local Postgres server using Docker and
Pipenv.

**You are welcome to utilize other package managers, or a simple `requirements.txt` file if
preferred.**

If you choose to utilize the included Docker+Pipenv implementation, use the following steps to
set up the local environment.

Your system needs to be able to:
 - Run `docker-compose` (you will need an up-to-date version of `docker` installed).
 - Run a `bash` script.

Everything is dockerized so as long as you are running an up-to-date version of docker
then everything will work. The automated spin up maps the app port to `8000` for
convenience.

### Basic Spin-up

Run `./scripts/run-local`

This will spin up a `postgres` container and an `app` container that is running a bare
Django app on a hot-loaded debug server. After spin-up, the command will run the database
migrations. This does **not** seed any users or super users.

### Adding Additional Dependencies

When adding a dependency to the `pipfile`, make sure to rerun `./scripts/run-local`, or run
`docker-compose build`, or Docker will not properly pull in the new dep.

### Notes

I added the dependency for jsonschema for validation.
There are 3 main routes:
 - POST `/batch/body` with raw JSON as the request body
 - GET `/batch/object/{object_id}` with the object ID as the last component of the path
 - GET `/batch/object_list` with query parameters `key` and `value`
The last choice is a bit controversial.  It is an easy, obvious one for
debugging and quick implementation, but query parameters are
exposed.  A POST (or perhaps a PUT - people can argue both ways)
would be a better choice, because the input parameters could be in the
request body, where HTTPS protects them from casual viewing.

There are remnants of things I've tried and decided against doing,
whether for time constraints or other reasons.  There are no doubt
failures in corner-cases  that better (any) unit testing would turn up.


