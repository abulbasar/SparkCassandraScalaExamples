CREATE keyspace lab WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE stocks ( 
      date TIMESTAMP,OPEN DOUBLE, 
      high DOUBLE, 
      low DOUBLE,CLOSE DOUBLE, 
      volume DOUBLE, 
      adjclose DOUBLE, 
      symbol text, 
      PRIMARY KEY((symbol), date) 
 ) WITH clustering ORDER BY (date DESC );
 
COPY stocks(date timestamp, open, high, low, close, volume, adjclose, symbol) FROM 'stocks.csv' WITH header = true
  AND datetimeformat = '%Y-%m-%d';

CREATE TABLE stocks_by_year_month 
             ( 
                          date TIMESTAMP, 
                          adjclose DOUBLE,CLOSE DOUBLE, 
                          high DOUBLE, 
                          low DOUBLE,OPEN DOUBLE, 
                          volume DOUBLE, 
                          symbol text, 
                          year  int, 
                          month int, 
                          PRIMARY KEY ((symbol, year, month), date) 
             );
