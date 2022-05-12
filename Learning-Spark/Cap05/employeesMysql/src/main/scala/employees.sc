import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val spark = SparkSession
  .builder
  .master("local[*]")
  .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def readFromMySql(db: String, dbTable: String)  = {

  val url = "jdbc:mysql://localhost:3306/"+db
  spark
    .read
    .format("jdbc")
    .option("url", url)
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", dbTable)
    .option("user", "root")
    .option("password", "G3hkPfPq")
    .load()
}

val employees = readFromMySql(
  "employees",
  "employees")

val departments = readFromMySql(
  "employees",
  "departments")

val deptEmp = readFromMySql(
  "employees",
  "dept_emp")

val salaries = readFromMySql(
  "employees",
  "salaries")

val titles = readFromMySql(
  "employees",
  "titles")

val foo = employees

val foo2 = foo
  .join(deptEmp,
    deptEmp("emp_no")===foo("emp_no"))
  .join(departments,
    departments("dept_no")===deptEmp("dept_no"))
  .join(titles,
    titles("emp_no")===deptEmp("emp_no") )
  .filter(
    datediff(to_date(titles("from_date")),to_date(deptEmp("from_date")))>= 0 &&
      datediff(to_date(titles("to_date")),to_date(deptEmp("to_date")))<= 0 &&
      datediff(to_date(titles("from_date")),to_date(deptEmp("to_date")))<= 0 &&
      datediff(to_date(titles("to_date")),to_date(deptEmp("from_date")))>= 0
    )
  .join(salaries,
    salaries("emp_no")===deptEmp("emp_no"))
  .filter(
      datediff(to_date(salaries("from_date")),to_date(titles("from_date")))>= 0 &&
      datediff(to_date(salaries("to_date")),to_date(titles("to_date")))<= 0 &&
      datediff(to_date(salaries("from_date")),to_date(titles("to_date")))<= 0 &&
      datediff(to_date(salaries("to_date")),to_date(titles("from_date")))>= 0
  )
  .select(
    foo("emp_no"),
    foo("birth_date"),
    foo("first_name"),
    foo("last_name"),
    foo("gender"),
    foo("hire_date"),
    deptEmp("from_date"),
    deptEmp("to_date"),
    departments("dept_no"),
    departments("dept_name"),
    titles("title"),
    titles("from_date"),
    titles("to_date"),
    salaries("salary"),
    salaries("from_date").as("from_date_salaries"),
    salaries("to_date")
  )
  .orderBy("emp_no", "from_date_salaries")


foo2.show(50)

