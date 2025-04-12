INSERT OVERWRITE TABLE ${hivevar:database}.sales_org_na_dim
SELECT
	 tvko.sales_org_code AS sales_organization
	,tvko.company_code AS company_code_of_the_sales_organization
	,t001.country_code AS country_key
	,t001.currency_code AS currency_key
	,t001.fiscal_year_variant_code AS fiscal_year_variant
	,tvko.customer_id AS customer_number_for_intercompany_billing
	,tvko.currency_code AS statistics_currency
FROM ${hivevar:dbOsiNa}.sales_org_dim AS tvko
	LEFT JOIN ${hivevar:dbOsiNa}.company_code_dim AS t001
		ON tvko.company_code = t001.company_code
;
