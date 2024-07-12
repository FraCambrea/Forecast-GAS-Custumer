# Forecast-GAS-Costumer
I built 3 forecast algorithm using Prophet python library to deal with portfolio gas demand. Gas market is not so easy to forecast due to few historical data. You need to be flexible in order to make different prevision based on how many information you have about the custumer. 
Much more time customer are in your portfolio, more accurate can be the forecast.
I built different algorithm:
- one with Prophet library for customer with more then 12 month of historical data
- one that work like a recalendar algorithm for customer have less then 12 month of consumption data

An other issue to deal with is that in the Gas market, in Italy, the consumption data are daily or monthly, based on the energy meter installed. I need to turn to daily also the monthly ones. I deal with that during the data preparation phase using standar profiles given by Snam autority and it's not explained in the code i published.

The alhorithms takes 3 parameters from command line, start date, end date, and flag char means which part of code execute. Parameter insert by command line are used to execute this code from a stored procedure, located in a database SQL Server, that simulate the program execution from windows command prompt.
