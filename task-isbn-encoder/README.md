# isbn-encoder

The function should check if the content in column _isbn_ is a [valid 13-digit isbn code](https://en.wikipedia.org/wiki/International_Standard_Book_Number) and create new rows for each part of the ISBN code.

#### Describe your solution

ISBN Encoder program is built using the Spark Dataframe API which offers high-level domain-specific operations, saves space, and executes at high speed. Hence it is better to use than RDD. In Dataframe, data is organized into named columns. Ex. table in a relational database. It is an immutable distributed collection of data allowing higher-level abstraction. 

The program as a whole takes an input in a single row containing Name, Year and ISBN number as input.
It then checks whether the ISBN number is a valid 13 digit code or not, and produces the Output as described below.
Note: The ISBN validation is done based on the standard.

def explodeIsbn() function takes an input in the following manner:

- Isbn(name: String, year: Int, isbn: String)

- Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1449358624")

It then stores the data in a temporary table "IsbnTable".
A query using Sql Context is made to fetch the ISBN column data and necessary regex calucation is performed to slice the 
ISBN number into its respective four parts: 

1. ISBN-EAN:
2. ISBN-GROUP:
3. ISBN-PUBLISHER:
4. ISBN-TITLE:

Finally it uses the explode function to create new rows for each part of the ISBN code as discussed above:
It takes in Input parameter as: Seq(("Learning Spark: Lightning-Fast Big Data Analysis", 2015, Seq("ISBN: 978-1449358624",ean,grp,pub,tit)))) where EAN_NO, Group, Publisher & title are the variables used to denote the 4 parts of ISBN number.
The explode function uses syntax: "ISBN", explode($"ISBN") where ISBN is the column name.

The final required output is displayed in the "finaltable" 


### Example

#### Input

| Name        | Year           | ISBN  |
| ----------- |:--------------:|-------|
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN: 978-1449358624 |

#### Output

| Name        | Year           | ISBN  |
| ----------- |:--------------:|-------|
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN: 978-1449358624 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-EAN: 978 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-GROUP: 14 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-PUBLISHER: 4935 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-TITLE: 862 |