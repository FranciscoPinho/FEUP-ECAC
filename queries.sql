// CLUSTERING 

SELECT COUNT(client.district_id) as nr_clients,district.* 
FROM `district` 
LEFT JOIN client on client.district_id=district.id
GROUP BY client.district_id,district.id  
ORDER BY `district`.`region` ASC

SELECT client.`id`,
client.`most_recent_balance`,
district.`name`,
district.`region`,
district.`num_inhabitants`,
district.`ratio_urban_inhab`,
district.`avg_salary`,
AVG(district.`unemployment_rate_95`+`district`.`unemployment_rate_96`)/2 as AVG_UNEMPLOYMENT,
district.`num_entrepeneurs_per1000_inhab`,
transactions.`nr_transactions`,
transactions.`credit_transactions`,
transactions.`withdrawal_transactions`,
transactions.`average_transactions_credit`,
transactions.`average_transactions_withdrawal`
FROM client
LEFT JOIN district on client.`district_id`=`district`.`id`
LEFT JOIN transactions on client.`id`=transactions.`client_id`
WHERE most_recent_balance is not null
GROUP BY client.`id`,
client.`most_recent_balance`,
transactions.`nr_transactions`,
transactions.`credit_transactions`,
transactions.`withdrawal_transactions`,
transactions.`average_transactions_credit`,
transactions.`average_transactions_withdrawal`,
district.`name`,
district.`region`,
district.`num_inhabitants`,
district.`ratio_urban_inhab`,
district.`avg_salary`,
district.`num_entrepeneurs_per1000_inhab`
ORDER BY client.`most_recent_balance` DESC

create view transactions as (
select `banking`.`disposition`.`client_id` AS `client_id`,`banking`.`trans_union`.`account_id` AS `account_id`,
`banking`.`disposition`.`type` AS `type`,
count(`banking`.`trans_union`.`account_id`) AS `nr_transactions`,
sum((`banking`.`trans_union`.`type` like '%credit%')) AS `credit_transactions`,
sum((`banking`.`trans_union`.`type` like '%withdrawal%')) AS `withdrawal_transactions`,
AVG(CASE WHEN `banking`.`trans_union`.`type` like '%credit%' THEN amount END) as `average_transactions_credit`,
AVG(CASE WHEN `banking`.`trans_union`.`type` like '%withdrawal%' THEN amount END) as `average_transactions_withdrawal`
from (`banking`.`trans_union` left join `banking`.`disposition` on((`banking`.`trans_union`.`account_id` = `banking`.`disposition`.`account_id`))) 
where (`banking`.`disposition`.`type` <> 'DISPONENT') 
group by `banking`.`trans_union`.`account_id`,`banking`.`disposition`.`client_id`,`banking`.`disposition`.`type`
)

// PREDICTION

//test query
SELECT 
loan_test.id,
loan_test.date,
loan_test.amount,
loan_test.duration,
loan_test.payments,
loan_test.status,
IFNULL(card_union.type,'nocard'),
IFNULL(card_union.issued,'1000-01-01'),
account.frequency,
account.has_disponent,
IFNULL(account.disponent_decade,0) as disponent_decade,
IFNULL(account.disponent_min_balance,0) as disponent_min_balance,
IFNULL(account.disponent_max_balance,0) as disponent_max_balance,
IFNULL(account.disponent_recent_balance,0) as disponent_recent_balance,
IFNULL(account.disponent_max_withdrawal,0) as disponent_max_withdrawal,
IFNULL(account.disponent_max_credit,0) as disponent_max_credit,
client.most_recent_balance,
client.decade,
IFNULL(client.max_withdrawal,0) as max_withdrawal,
IFNULL(client.max_credit,0) as max_credit,
IFNULL(client.min_withdrawal,0) as min_withdrawal,
IFNULL(client.min_credit,0) as min_credit,
client.`min_balance`,
client.`max_balance`,
(SELECT district.`region` FROM district WHERE district.id=account.district_id ) as accdist_region,
(SELECT district.`num_inhabitants` FROM district WHERE district.id=account.district_id ) as accdist_numinhab,
(SELECT district.`ratio_urban_inhab` FROM district WHERE district.id=account.district_id ) as accdist_urbanratio,
(SELECT district.`avg_salary` FROM district WHERE district.id=account.district_id ) as accdist_avgsalary,
(SELECT AVG(district.`num_crimes_95`+`district`.`num_crimes_96`)/2 FROM district WHERE district.id=account.district_id ) as accdist_avgcrimes,
(SELECT AVG(district.`unemployment_rate_95`+`district`.`unemployment_rate_96`)/2 FROM district WHERE district.id=account.district_id ) as accdist_avgunemployment,
(SELECT district.`num_entrepeneurs_per1000_inhab` FROM district WHERE district.id=account.district_id ) as accdist_entrepeneurs,
district.`region` as clidist_region,
district.`num_inhabitants` as clidist_numinhab,
district.`ratio_urban_inhab` as clidist_urbanratio,
district.`avg_salary` as clidist_avgsalary,
AVG(district.`num_crimes_95`+`district`.`num_crimes_96`)/2 as clidist_avgcrimes,
AVG(district.`unemployment_rate_95`+`district`.`unemployment_rate_96`)/2 as clidist_avgunemployment,
district.`num_entrepeneurs_per1000_inhab` as clidist_entrepeneurs,
transactions.`average_transactions_credit`,
transactions.`average_transactions_withdrawal` 
FROM `loan_test` 
LEFT JOIN disposition on loan_test.account_id=disposition.account_id
LEFT JOIN card_union on disposition.id=card_union.disposition_id
LEFT JOIN account on disposition.account_id=account.id
LEFT JOIN client on disposition.client_id=client.id
LEFT JOIN transactions on transactions.client_id=disposition.`client_id`
LEFT JOIN district on client.`district_id`=`district`.`id` 
where (`banking`.`disposition`.`type` <> 'DISPONENT') 
GROUP BY loan_test.id,
disposition.client_id,
card_union.type,
card_union.issued,
transactions.`average_transactions_credit`,
transactions.`average_transactions_withdrawal`

//train query
SELECT 
loan_train.id,loan_train.date,loan_train.amount,
loan_train.duration,
loan_train.payments,
loan_train.status,
IFNULL(card_union.type,'nocard') as card_type,
IFNULL(card_union.issued,'1000-01-01') as card_issued,
account.frequency,
account.has_disponent,
IFNULL(account.disponent_decade,0) as disponent_decade,
IFNULL(account.disponent_min_balance,0) as disponent_min_balance,
IFNULL(account.disponent_max_balance,0) as disponent_max_balance,
IFNULL(account.disponent_recent_balance,0) as disponent_recent_balance,
IFNULL(account.disponent_max_withdrawal,0) as disponent_max_withdrawal,
IFNULL(account.disponent_max_credit,0) as disponent_max_credit,
client.most_recent_balance,
client.decade,
IFNULL(client.max_withdrawal,0) as max_withdrawal,
IFNULL(client.max_credit,0) as max_credit,
IFNULL(client.min_withdrawal,0) as min_withdrawal,
IFNULL(client.min_credit,0) as min_credit,
client.`min_balance`,
client.`max_balance`,
(SELECT district.`region` FROM district WHERE district.id=account.district_id ) as accdist_region,
(SELECT district.`num_inhabitants` FROM district WHERE district.id=account.district_id ) as accdist_numinhab,
(SELECT district.`ratio_urban_inhab` FROM district WHERE district.id=account.district_id ) as accdist_urbanratio,
(SELECT district.`avg_salary` FROM district WHERE district.id=account.district_id ) as accdist_avgsalary,
(SELECT AVG(district.`num_crimes_95`+`district`.`num_crimes_96`)/2 FROM district WHERE district.id=account.district_id ) as accdist_avgcrimes,
(SELECT AVG(district.`unemployment_rate_95`+`district`.`unemployment_rate_96`)/2 FROM district WHERE district.id=account.district_id ) as accdist_avgunemployment,
(SELECT district.`num_entrepeneurs_per1000_inhab` FROM district WHERE district.id=account.district_id ) as accdist_entrepeneurs,
district.`region` as clidist_region,
district.`num_inhabitants` as clidist_numinhab,
district.`ratio_urban_inhab` as clidist_urbanratio,
district.`avg_salary` as clidist_avgsalary,
AVG(district.`num_crimes_95`+`district`.`num_crimes_96`)/2 as clidist_avgcrimes,
AVG(district.`unemployment_rate_95`+`district`.`unemployment_rate_96`)/2 as clidist_avgunemployment,
district.`num_entrepeneurs_per1000_inhab` as clidist_entrepeneurs,
transactions.`average_transactions_credit`,
transactions.`average_transactions_withdrawal` 
FROM `loan_train` 
LEFT JOIN disposition on loan_train.account_id=disposition.account_id
LEFT JOIN card_union on disposition.id=card_union.disposition_id
LEFT JOIN account on disposition.account_id=account.id
LEFT JOIN client on disposition.client_id=client.id
LEFT JOIN transactions on transactions.client_id=disposition.`client_id`
LEFT JOIN district on client.`district_id`=`district`.`id`
where (`banking`.`disposition`.`type` <> 'DISPONENT') 
GROUP BY loan_train.id,
disposition.client_id,
card_union.type,
card_union.issued,
transactions.`average_transactions_credit`,
transactions.`average_transactions_withdrawal`