# database-project3
## Usage Example

To run the database implemention:
```
$ python3 main.py
```
The code takes commands like
```
>>> source <source_file>
```
where the source file has sql commands, or commands like 
```
>>> SELECT * FROM TABLE;
```
where the direct input is a sql statement. To exit the database interface type `exit`. 


## Demo 

Run the following to reproduce our demo outputs:

```
$ python3 main.py
sql> source demo_tables.sql
sql> source demo_queries.sql
sql> source country_income.sql
```
