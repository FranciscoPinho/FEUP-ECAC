from dplython import *
import pandas as pd
from ggplot import *

client_data = DplyFrame(pd.read_csv('ultimate.csv'))


salary_balance=(client_data >>
 select(X.average_regional_salary, X.most_recent_balance,X.average_transactions_credit))

unemployment_balance=(client_data >> 
 select(X.average_unemployment, X.most_recent_balance,X.average_transactions_credit))

urban_ratio_balance=(client_data >> 
 select(X.ratio_urban_inhab, X.most_recent_balance,X.average_transactions_credit))

entrepeneurs_balance=(client_data >> 
 select(X.num_entrepeneurs_per1000_inhab, X.most_recent_balance,X.average_transactions_credit))

region_balance=(client_data >> 
 select(X.region, X.most_recent_balance,X.average_transactions_credit))

revenue_balance=(client_data >>
 select(X.average_transactions_credit, X.most_recent_balance))

withdrawals_balance=(client_data >>
select(X.average_transactions_withdrawal, X.most_recent_balance, X.average_transactions_credit))

number_credits=(client_data >>
 select(X.credit_transactions, X.most_recent_balance,X.average_transactions_credit))

number_withdrawals=(client_data >>
select(X.withdrawal_transactions, X.most_recent_balance, X.average_transactions_credit))

ggplot = DelayFunction(ggplot)  # Simple installation
plot = (ggplot(aes(x="average_regional_salary", y="most_recent_balance", color="average_transactions_credit"), data=salary_balance) 
        + geom_point() + scale_color_gradient(low="coral", high="steelblue") + labs(x="District Average Salary", y="Most Recent Balance"))
plot.save('salarybalance.png')

plot = (ggplot(aes(x="average_unemployment", y="most_recent_balance", color="average_transactions_credit"), data=unemployment_balance) 
        + geom_point() + scale_color_gradient(low="coral", high="steelblue") + labs(x="District Average Unemployment", y="Most Recent Balance"))
plot.save('unemploybalance.png')

plot = (ggplot(aes(x="ratio_urban_inhab", y="most_recent_balance", color="average_transactions_credit"), data=urban_ratio_balance) 
        + geom_point() + scale_color_gradient(low="coral", high="steelblue") + labs(x="District Urban Ratio", y="Most Recent Balance"))
plot.save('urbanbalance.png')

plot = (ggplot(aes(x="num_entrepeneurs_per1000_inhab", y="most_recent_balance", color="average_transactions_credit"), data=entrepeneurs_balance) 
        + geom_point() + scale_color_gradient(low="coral", high="steelblue") + labs(x="District Entrepeneurs per 1k", y="Most Recent Balance"))
plot.save('entrebalance.png')

plot = (ggplot(aes(x="average_transactions_credit", y="most_recent_balance"), data=revenue_balance) 
        + geom_point() + scale_color_gradient(low="coral", high="steelblue") + labs(x="Average Value per Credit Transaction", y="Most Recent Balance"))
plot.save('revenuebalance.png')

plot = (ggplot(aes(x="average_transactions_withdrawal", y="most_recent_balance"), data=withdrawals_balance) 
        + geom_point() + scale_color_gradient(low="coral", high="steelblue") + labs(x="Average Value per Withdrawal Transaction", y="Most Recent Balance"))
plot.save('withdrawalbalance.png')

plot = (ggplot(aes(x="credit_transactions", y="most_recent_balance"), data=number_credits) 
        + geom_point() + scale_color_gradient(low="coral", high="steelblue") + labs(x="Number of Credit Transactions", y="Most Recent Balance"))
plot.save('nrcreditsbalance.png')

plot = (ggplot(aes(x="withdrawal_transactions", y="most_recent_balance"), data=number_withdrawals) 
        + geom_point() + scale_color_gradient(low="coral", high="steelblue") + labs(x="Number of Withdrawal Transactions", y="Most Recent Balance"))
plot.save('nrwithdrawalbalance.png')