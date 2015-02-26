desmond
=========
Reusable background job functionalety. Building on top of that:
Importing/exporting functionality for the RedShift data warehouse.


Installation
---------------------
Desmond hooks into db:migrate, so just execute `rake db:migrate` and it will copy its migrations and execute them.

Configuration
---------------------
Desmond expects the file `config/desmond.yml` to exist and uses that to determine where it can find the other configuration files it needs. See `config/desmond.sample.yml` for options and their defaults.

Usage
---------------------
`Desmond::BaseJob` is the base class for all jobs build on top of this library. You can schedule any job for background processing using its `enqueue` class method. To run any job synchronously use `run`. They both take an ID identifying that type of job and an ID identifying the user this is executed for, as well as an arbitrary hash of options.

`enqueue` returns a JobRun instance which you can use to check the status of the job. `Demond::JobRunFinders` can be used on any job to find the JobRun instance again if you don't have it available anymore (using the provided job & user ID). The return value of any job needs to be representable by JSON (no custom objects). The return value can be retrieved from the JobRun instance using its `result` method. Running a job synchronously returns the jobs return value directly.

If an error (exception is thrown or job marks itself as failed) occurs the error can be retrieved using the method `error` on the JobRun. `failed?` will also return true. The synchronous interface will throw an exception with the error message.

Check `lib/desmond/jobs/template*` for example son how to implement your own jobs.

`Desmond::ExportJob` can be used to export data to CSV out of a PostgreSQL-compatible database. See it's method documentation or exmaples/* for the options you'll need to provide.

`Desmond::ImportJob` can be used to import a CSV into RedShift. See it's method documentation or exmaples/* for the options you'll need to provide.


Hooks
---------------------
Each job will run the following hooks if available in this order:
- before
- [ ... execution ...]
- success/error
- after

You can subclass an existing job to define these hooks for a particular job.

Development
---------------------
For development on Desmond make sure you run `rake db:migrate` to create Que's job table and run Desmond's migrations.

Modify `config/tests.yml` for your environment, then run `rspec` to check your changes against conventions.

