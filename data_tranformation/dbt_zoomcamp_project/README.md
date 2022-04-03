Welcome to your new dbt project!

### Using the starter project

Try running the following commands:

- dbt run
- dbt test

#### Execution

[Reference](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/taxi_rides_ny/README.md)

After having installed the required tools and cloning this repo, execute the following commnads:

1. Change into the project's directory from the command line: `$ cd data_transformation/dbt_zoomcamp_project`
2. Load the CSVs (data/product_lookup.csv) into the database. This materializes the CSVs as tables in your target schema: `$ dbt seed`
3. Run the models: `$ dbt run`
4. Test your data: `$ dbt test`
   _Alternative: use `$ dbt build` to execute with one command the 3 steps above together_
5. Generate documentation for the project: `$ dbt docs generate`
6. View the documentation for the project, this step should open the documentation page on a webserver, but it can also be accessed from http://localhost:8080 : `$ dbt docs serve`

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](http://community.getbdt.com/) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
