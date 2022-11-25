Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


Mapping schemas table view, columns, structure I need to accomplish the task
--SELECT TOP 100 * FROM PREP.SALESFORCE.ACCOUNT;
--SELECT TOP 100 * FROM PREP.SALESFORCE.USER;

SELECT TOP 100 * FROM PREP.HZ_CUST_ACCOUNTS_STAGE 

EFS_CUSTOMER_STAGE (LEGACY_DATA_LAKE_CRD_STAGE)
REPD_ACCOUNT_STAGE (LEGACY_DATA_LAKE_EDW_STAGE)
HZ_CUST_ACCOUNTS_STAGE (LEGACY_DATA_LAKE_EBS_STAGE)
