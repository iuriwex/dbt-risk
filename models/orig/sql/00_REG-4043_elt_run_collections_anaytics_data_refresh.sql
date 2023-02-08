/* Script create date: 2021.01.07 Report Author: NMorrill; PLiberatore             */
/* Script Audited by: MGanesh         Report Audited Date: 2021-01-06              */
\i 01_REG-4043_elt_run_collections_anaytics_data_refresh_20210107.sql

/*  Nick Bogan, 2021-04-23: When we build ca_driver, we don't bother doing a full
 * outer join of case and sf_case_history, because ca_driver is inner joined to
 * case when loading ca_collection_cases. 
 
 * added materoialized views  iuriwex 20221223
 */


\i 01_REG-4043_elt_run_collections_anaytics_data_refresh_20210423.sql

/* Created By: MGanesh;
* Audited By: NMorrill
* Purpose: Creates Segment Analysis Dataset that will be used for collection analysis and visualization
*/
\i 01_REG-4043_elt_run_collections_anaytics_data_refresh_20200111.sql
