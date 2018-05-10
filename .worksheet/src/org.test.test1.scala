package org.test

object test1 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(77); 
  println("Welcome to the Scala worksheet");$skip(12); ;
  var a =10;System.out.println("""a  : Int = """ + $show(a ));$skip(33); 
  val zeros = Seq.fill(10000)(0);System.out.println("""zeros  : Seq[Int] = """ + $show(zeros ))}
  
 
}
