desmond
=========
Importing/exporting functionality for the RedShift data warehouse


Installation
---------------------
Run `rake desmond:migrate` to create the necessary migrations in your migration folder 'db/migrate'.

Configuration
---------------------
Desmond expects the file 'config/desmond.yml' to exist and uses that to determine where it can find the other configuration files it needs. See 'config/desmond.sample.yml' for options and their default.

Usage
---------------------
Schedule an export using the 'enqueue' class method of 'Desmond::ExportJob'. You'll need to supply an unique identifier for the job and the user executing the task, so you can check the execution status later on. For more information on additional required arguments check 'examples/raw.rb'.
To check the execution status later on the methods of 'Demond::JobRunFinders' are available in 'Desmond::ExportJob'.
