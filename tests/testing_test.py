
from source.etl_kommati_para import ETLKommatiPara

test_etl = ETLKommatiPara()

df_in = test_etl.spark.createDataFrame([(1, 2., 'string1', 'string 4'),
                                            (2, 3., 'string2', 'string 5'),
                                            (3, 4., 'string3', 'string 6')], 
                                            schema = 'a long, b double, c string, d string').show()