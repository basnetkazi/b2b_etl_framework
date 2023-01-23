import logging
import sys
##sys.path.append('../')

from bin import file_format
from bin.extraction import company_ex, customer_ex, order_ex, order_delta_ex, product_ex, price_ex, extraction_batch_end,extraction_batch_start, web_log_ex
from bin.load import company_ld, customer_ld, order_ld,product_ld,price_ld, load_batch_start,load_batch_end, web_log_ld
#truncate_tables.truncate()
# file_format.create_file_format()

from bin import log_generator

#Extraction Block
extraction_batch_start.start_extract()
log_generator.log_generator()
product_ex.product_extract()
company_ex.company_extract()
customer_ex.customer_extract()
price_ex.price_extract()
order_ex.order_extract()
order_delta_ex.order_delta_extract()
extraction_batch_end.end_extract()
web_log_ex.web_log_extract()




#Load_Block
load_batch_start.start_load()
product_ld.product_load()
company_ld.company_load()
customer_ld.customer_load()
price_ld.price_load()
order_ld.order_load()
load_batch_end.end_load()
web_log_ld.web_log_load()


