import happybase # importing happybase library

# creating connection
connection = happybase.Connection('localhost', port=9090, autoconnect=False)

# opening connection to perform operations
def open_connection():
    connection.open()

# closing opened connection 
def close_connection():
    connection.close()

# getting the pointer to a table
def get_table(name):
    open_connection()
    table = connection.table(name)
    close_connection()
    return table

def batch_insert_data(filename, tablename): # batch insert data into the table
    print("starting batch insert of "+filename)
    file = open(filename, 'r')
    table = get_table(tablename)
    open_connection()
    i = 0
    with table.batch(batch_size=50000) as b:
        for line in file:
            if i!=0:
                temp = line.strip().split(",")
                b.put(temp[1]+temp[2] ,{'col1:VendorID': str(temp[0]), 'col1:tpep_pickup_datetime': str(temp[1]), 'col1:tpep_dropoff_datetime': str(temp[2]), 'col1:passenger_count': str(temp[3]), 'col1:trip_distance': str(temp[4]), 'col1:RatecodeID': str(temp[5]), 'col1:store_and_fwd_flag': str(temp[6]), 'col1:PULocationID': str(temp[7]), 'col1:DOLocationID': str(temp[8]), 'col1:payment_type': str(temp[9]),'col1:fare_amount': str(temp[10]), 'col1:extra': str(temp[11]), 'col1:mta_tax': str(temp[12]), 'col1:tip_amount': str(temp[13]), 'col1:tolls_amount': str(temp[14]), 'col1:improvement_surcharge': str(temp[15]), 'col1:total_amount': str(temp[16]), 'col1:congestion_surcharge': str(temp[17]), 'col1:airport_fee': str(temp[18]) })
            
            i+=1

    file.close()
    print("batch insert done")
    close_connection()    

batch_insert_data('yellow_tripdata_2017-03.csv', 'trip_records')
batch_insert_data('yellow_tripdata_2017-04.csv', 'trip_records')