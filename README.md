# OakNorth DBT Home Test

## The challenge
As part of the application process, we would like you to complete a take-home exercise to demonstrate your proficiency in DBT (Data Build Tool) and your analytical skills. This exercise is designed to assess your ability to work on data transformations and modelling using DBT, a tool that is widely used in our organisation for data analytics.

## Scenario
You are provided with two seeds: "transactions" and "customers." The seeds contain representative sample data from our banking system. The "transactions" seed contains information about financial transactions, and the "customers" seed contains information about the bank's customers. Your task is to use DBT to fulfill the following criteria:

- Create a model that consolidates relevant customer information with their transactions:
    - total transactions made by each customer
    - their average monthly spending
    - the average customer spending interval, representing the average number of days it takes for the customer to perform a transaction
- Create a model that calculates the total transaction amount for each month over the last 12 months
    - calculate the percentage change in transaction totals from one month to the next
    - calculate the top customer for each month by selecting the customer with the highest spending

## Submission Instructions
1. Fork this GitHub repository: https://github.com/oaknorthbank/dbt-home-test
2. Create a DBT project within the repository and set up your DBT environment.
3. Develop the necessary DBT models and SQL queries to complete the exercise.
4. Document your code and provide explanations where necessary.
5. Test your models using sample data to validate their correctness.
6. Update the README.md file in the repository to include instructions on how to run your DBT project and tests.
7. Commit and push your changes to your GitHub repository.

Please follow the guidelines below:
- Use DBT's best practices for data modeling, such as using unique keys, documenting your models, etc.
- Write SQL queries within DBT models to perform the required data transformations.
- Test your models to ensure they are working as expected.

### Additional terms
- Please do not use external DBT packages
- You may of course use any resources you like to assist you with specific techniques, syntax etc - but please do not just copy code.
- Please don't share this exercise with anyone else :)

## Instructions on how to run this DBT project and tests
 - dbt seed 
 - dbt run 
 - dbt test 
### Additional notes on tests 
 - There are two custom tests testing data quality that throw warnings when they fail!