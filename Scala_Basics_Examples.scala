// ==========================================
// Scala Practice Session – GitHub Ready
// Date: 2025-08-21
// ==========================================

/*
  Spark and Scala environment info:
  WARN  Utils: Your hostname resolves to a loopback address; using local IP instead.
  Spark context Web UI: http://192.168.64.5:4040
  Spark version: 2.3.1
  Scala version: 2.11.8
  Java version: 1.8.0_462
*/

// -------------------------
// Basic Print and Variables
// -------------------------
val printMe = "hello world"
println(printMe)  // Output: hello world

// -------------------------
// Simple Sum Functions
// -------------------------
def sum(a: Int, b: Int): Int = {
  return a + b
}

println(sum(10, 9))   // Output: 19
println(sum(90, 10))  // Output: 100

// -------------------------
// Odd/Even Checker
// -------------------------
def checkOdd(a: Int): Unit = {
  if(a % 2 == 0) println(s"$a is even")
  else println(s"$a is odd")
}

checkOdd(5)  // Output: 5 is odd

def checkEven(): Unit = {
  import scala.io.StdIn
  println("Enter a number:")
  val a = StdIn.readInt()  // read integer input from user
  if (a % 2 == 0) println(s"$a is even")
  else println(s"$a is odd")
}

// checkEven()  // Uncomment to run interactively

// -------------------------
// String Operations
// -------------------------
val str = "Arpit"
println(str.length)  // Output: 5

val str1 = "Arpit"
val str2 = "rajkumar"
println(str1 + str2)  // Output: Arpitrajkumar

var name = "Chakri"
println(s"Hello dear $name")  // Output: Hello dear Chakri

// -------------------------
// Number Formatting
// -------------------------
val pi = 3.14159f
println(f"The value of pi is $pi%.2f")  // Output: The value of pi is 3.14

// -------------------------
// Raw vs Regular Strings
// -------------------------
println("without Raw:\nFirst\nSecond")
println(raw"with Raw:\nFirst\nSecond")

// -------------------------
// String Comparison
// -------------------------
var str3 = "PQR"
var str4 = "pqr"
println(str3 == str4)  // Output: false
