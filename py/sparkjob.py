from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import DoubleType, IntegerType,FloatType
from pyspark.sql.functions import udf

#import mysql.connector
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/adityaallamraju/hadoop-install/mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar  pyspark-shell'

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/arpit.maheshwari/Downloads/mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar  pyspark-shell'

# sys.path.append("/Users/arpit.maheshwari/Downloads/mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar")

conf = SparkConf()\
    .setAppName("Ingest Data to Data zalora catalog")\
  #  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

sc = SparkContext(conf=conf)
sqlContext =SQLContext(sc)

#spark.driver.extraClassPath /Users/arpit.maheshwari/mysqlconnector/mysql-connector-java-5.1.45-bin.jar
#spark.executor.extraClassPath /Users/arpit.maheshwari/mysqlconnector/mysql-connector-java-5.1.45-bin.jar
# --driver-class-path

#cnx = mysql.connector.connect(user='guest', password='relational',
#                              host='relational.fit.cvut.cz',
#                             database='northwind')
#cnx.close()


properties = {
    "url": "jdbc:mysql://relational.fit.cvut.cz:3306/northwind",
    "driver": "com.mysql.jdbc.Driver",
    "user":"guest",
    "password":"relational",
    "numPartitions":"1"
}

# val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
def main():

    # sqlContext.read.format("jdbc").options(
    #     url="jdbc:mysql://relational.fit.cvut.cz:3306/northwind",
    #     driver="com.mysql.jdbc.Driver",
    #     dbtable="Employees",
    #     user="guest",
    #     password="relational"
    #     # numPartitions=2
    #
    # ).load().show(10)


# ************* Will be joining the Datasets below to create a Denormalized Dataset --- DetailedOrders**************

    OrderDetailsDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="`Order Details`"
    ).load()

    OrdersDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="Orders"
    ).load()

    ShippersDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="Shippers"
    ).load()

# ************* Will be joining the Datasets below to create a Denormalized Dataset --- Customers**************


    CustomersDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="Customers"
    ).load()

    CustomerCustomerDemoDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="CustomerCustomerDemo"
    ).load()

    CustomerDemographicsDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="CustomerDemographics"
    ).load()


# ************* Will be joining the Datasets below to create a Denormalized Dataset --- Employees**************

    EmployeesDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="Employees"
    ).load()

    EmployeeTerritoriesDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="EmployeeTerritories"
    ).load()

    TerritoriesDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="Territories"
    ).load()


# ************* Will be joining the Datasets below to create a Denormalized Dataset --- Products**************

    ProductsDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="Products"
    ).load()

    SuppliersDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="Suppliers"
    ).load()

    CategoriesDF = sqlContext.read.format("jdbc").options(
        **properties).options(dbtable="Categories"
    ).load()

    def total_price(UnitPrice,Quantity,Discount):
        print ('UnitPrice')
        print (UnitPrice)
        print('Discount')
        print(Discount)
        print('Quantity')
        print(Quantity)
        total_price=(UnitPrice*Quantity)-Discount
        print ('tp')
        print (total_price)
        total_price

    total_price_udf = udf(total_price,DoubleType())

# ************* Denormalizing the datasets ************

    DetailedOrdersDF = OrderDetailsDF.join(OrdersDF,
                                         (OrderDetailsDF.OrderID == OrdersDF.OrderID),
                                         'inner').drop(OrdersDF.OrderID
                                         ).join(ShippersDF,
                                                (OrdersDF.ShipVia == ShippersDF.ShipperID),'inner').drop(OrdersDF.ShipVia)

   #DetailedOrdersDF.write.csv("DetailedOrdersDF")
  #  DetailedOrdersDF.withColumn("total_price",total_price_udf('UnitPrice','Quantity','Discount')).show(10)
  #  DetailedOrdersDF.withColumn("total_price1",total_price_udf(DetailedOrdersDF['UnitPrice'].cast('double'),DetailedOrdersDF['Quantity'],DetailedOrdersDF['Discount'])).show(10)
    DetailedOrdersDF_tp=DetailedOrdersDF.withColumn("total_price",(DetailedOrdersDF['UnitPrice'].cast('double')*DetailedOrdersDF['Quantity'])-DetailedOrdersDF['Discount'])
    DetailedOrdersDF_tp.show(10)
    DetailedOrdersDF_tp.registerTempTable('DetailedOrders')
    sqlContext.sql('select * from DetailedOrders').show()
    sqlContext.sql('select CustomerID,month(OrderDate) as month,sum(total_price) from DetailedOrders group by CustomerID,month').show(5)
    sqlContext.sql('select ProductID,count(distinct CustomerID) from DetailedOrders where total_price>=50 group by ProductID').show(5)
    # DetailedCustomersDF = CustomersDF.join(CustomerCustomerDemoDF,
    #                                       (CustomersDF.CustomerID == CustomerCustomerDemoDF.CustomerID),
    #                                       'inner').drop(CustomerCustomerDemoDF.CustomerID
    #                                      ).join(CustomerDemographicsDF,
    #                                         (CustomerCustomerDemoDF.CustomerTypeID == CustomerDemographicsDF.CustomerTypeID),
    #                                     'inner').drop(CustomerDemographicsDF.CustomerID).write.csv("DetailedCustomers")
    #
    # DetailedEmployeesDF = EmployeesDF.join(EmployeeTerritoriesDF,
    #                                        (EmployeesDF.EmployeeID == EmployeeTerritoriesDF.EmployeeID),
    #                                        'inner').drop(EmployeeTerritoriesDF.EmployeeID
    #                                     ).join(TerritoriesDF,
    #                                            (EmployeeTerritoriesDF.TerritoryID == TerritoriesDF.TerritoryID),
    #                                            'inner').drop(TerritoriesDF.TerritoryID).write.csv("DetailedEmployees")
    #
    # DetailedProductsDF = ProductsDF.join(SuppliersDF,
    #                                      (ProductsDF.SupplierID == SuppliersDF.SupplierID),
    #                                      'inner').drop(SuppliersDF.SupplierID
    #                                     ).join(CategoriesDF,
    #                                            (ProductsDF.CategoryID == CategoriesDF.CategoryID),
    #                                            'inner').drop(CategoriesDF.CategoryID).write.csv("DetailedProducts")



if __name__ == '__main__':
    main()

