package org.foundator.review

import java.util.concurrent.atomic.AtomicLong

import org.foundator.review.ScalaQuery.JoinType.Inner
import org.foundator.review.ScalaQuery.Value.IsOrdered

object ScalaQuery {

    abstract class Table(row : Table.Row) {
        def getRow = row
        abstract class Column[V] extends Table.RowColumn[this.type, V](this, row)
        abstract class ForeignKey[F <: Table, V](foreign : Table.Row => F, key : F => Table.RowColumn[F, V]) extends Table.RowForeignKey[this.type, F, V](this, row, foreign, key)
    }

    object Table {
        private val rowCounter = new AtomicLong(0)
        class Row private[ScalaQuery] () { val id = rowCounter.incrementAndGet() }
        private[ScalaQuery] abstract class RowForeignKey[+T, F, V](table : Table, row : Row, val foreign : Row => F, val key : F => RowColumn[F, V]) extends RowColumn[T, V](table, row)
        private[ScalaQuery] abstract class RowColumn[+T, V](val table : Table, val row : Row)
    }

    implicit def ColumnToValue[V](column : Table.RowColumn[_, V]) : Value[V] = Value.Scalar[V](column)
    implicit def IntToValue(value : Int) : Value[Int] = Value.Int32(value)
    implicit def LongToValue(value : Long) : Value[Long] = Value.Int64(value)

    implicit def IntIsOrdered(value : Int) : IsOrdered = ???
    implicit def LongIsOrdered(value : Long) : IsOrdered = ???

    sealed abstract class Value[V] extends ForSyntax.ForValue[V] {
        import Value._
        def ===(that : Value[V]) = Equals(this, that)
        def !==(that : Value[V]) = NotEquals(this, that)
        def >(that : Value[V])(implicit v : V => IsOrdered) = Greater(this, that)
        def <(that : Value[V])(implicit v : V => IsOrdered) = Less(this, that)
        def >=(that : Value[V])(implicit v : V => IsOrdered) = GreaterEquals(this, that)
        def <=(that : Value[V])(implicit v : V => IsOrdered) = LessEquals(this, that)
        def &&(that : Value[Boolean])(implicit ev : Value[V] =:= Value[Boolean]) = And(this, that)
        def ||(that : Value[Boolean])(implicit ev : Value[V] =:= Value[Boolean]) = Or(this, that)
    }
    object Value {
        final class IsOrdered

        case class Join[T <: Table, V](joinType : JoinType, table : T, on : T => Value[Boolean], body : T => Value[V]) extends Value[V]
        case class Where[V](condition : Value[Boolean], result : Value[V]) extends Value[V]
        case class Scalar[V](column : Table.RowColumn[_, V]) extends Value[V]
        case class Select2[V1, V2](v1 : Value[V1], v2 : Value[V2]) extends Value[(V1, V2)]
        case class Select3[V1, V2, V3](v1 : Value[V1], v2 : Value[V2], v3 : Value[V3]) extends Value[(V1, V2, V3)]
        case class Select4[V1, V2, V3, V4](v1 : Value[V1], v2 : Value[V2], v3 : Value[V3], v4 : Value[V4]) extends Value[(V1, V2, V3, V4)]

        case class Equals[V](v1 : Value[V], v2 : Value[V]) extends Value[Boolean]
        case class NotEquals[V](v1 : Value[V], v2 : Value[V]) extends Value[Boolean]

        case class Greater[V <% IsOrdered](v1 : Value[V], v2 : Value[V]) extends Value[Boolean]
        case class Less[V <% IsOrdered](v1 : Value[V], v2 : Value[V]) extends Value[Boolean]
        case class GreaterEquals[V <% IsOrdered](v1 : Value[V], v2 : Value[V]) extends Value[Boolean]
        case class LessEquals[V <% IsOrdered](v1 : Value[V], v2 : Value[V]) extends Value[Boolean]

        case class And(v1 : Value[Boolean], v2 : Value[Boolean]) extends Value[Boolean]
        case class Or(v1 : Value[Boolean], v2 : Value[Boolean]) extends Value[Boolean]

        case class Int32(value : Int) extends Value[Int]
        case class Int64(value : Long) extends Value[Long]
        case class Bool(value : Boolean) extends Value[Boolean]
        case class Null[V]() extends Value[V]
    }

    sealed abstract class Query[V]
    object Query {
        case class From[T <: Table, V](table : T, body : T => Value[V]) extends Query[V]
    }

    sealed abstract class JoinType
    object JoinType {
        case object Inner extends JoinType
        case object Left extends JoinType
        case object Right extends JoinType
        case object Full extends JoinType
        case object Cross extends JoinType
    }

    object ForSyntax {
        abstract class ForValue[V] { this : Value[V] =>
            def map[R](body : Value[V] => Value[R]) : Value[R] = body(this)
            def flatMap[R](body : Value[V] => Value[R]) : Value[R] = body(this)
            def filter(f : Value[V] => Value[Boolean]) : Value[V] = Value.Where(f(this), this)
        }

        private[ScalaQuery] class ForJoin[T <: Table](table : Table.Row => T, onCondition : T => Value[Boolean], joinType : JoinType) {
            protected val tableRow = table(new Table.Row)
            protected def addWhere[V](value : Value[V]) : Value[V] = value
            def map[V](body : T => Value[V]) : Value[V] = Value.Join(joinType, tableRow, onCondition, body.andThen(addWhere))
            def flatMap[V](body : T => Value[V]) : Value[V] = Value.Join(joinType, tableRow, onCondition, body.andThen(addWhere))
            def filter(f : T => Value[Boolean]) = new ForJoin[T](table, onCondition, joinType) {
                override val tableRow = ForJoin.this.tableRow
                override def addWhere[V](value : Value[V]) : Value[V] = ForJoin.this.addWhere(Value.Where(f(tableRow), value))
            }
            def on(condition : T => Value[Boolean]) = new ForJoin[T](table, t => Value.And(onCondition(t), condition(t)), joinType)
        }

        private[ScalaQuery] class ForFrom[T <: Table](table : Table.Row => T) {
            protected val tableRow = table(new Table.Row)
            protected def addWhere[V](value : Value[V]) : Value[V] = value
            def map[V](body : T => Value[V]) : Query[V] = Query.From(tableRow, body.andThen(addWhere))
            def flatMap[V](body : T => Value[V]) : Query[V] = Query.From(tableRow, body.andThen(addWhere))
            def filter(f : T => Value[Boolean]) = new ForFrom[T](table) {
                override val tableRow = ForFrom.this.tableRow
                override def addWhere[V](value : Value[V]) : Value[V] = ForFrom.this.addWhere(Value.Where(f(tableRow), value))
            }
        }
    }

    class Printer {
        import Value._
        val rowNames = scala.collection.mutable.Map[(String, Long), Long]()
        def rowName(text : String, rowId : Long) = {
            val prefix = text.toLowerCase.headOption.filter(c => c >= 'a' && c <= 'z').map(_.toString).getOrElse("q")
            val number = rowNames.getOrElse((prefix, rowId), {
                val filtered = rowNames.filterKeys(_._1 == prefix)
                val n = if(filtered.isEmpty) 1 else filtered.map(_._2).max + 1
                rowNames.put((prefix, rowId), n)
                n
            })
            prefix + number
        }
        def tableName(table : Object) = table.getClass.getName.split('$').reverse.tail.head

        def apply[V](query : Query[V]) : String = query match {
            case Query.From(table, body : (Table => Value[V])) => s"""FROM "${tableName(table)}" ${rowName(tableName(table), table.getRow.id)}""" + apply(body(table))(false)
        }

        def apply[V](value : Value[V])(implicit whereAnd : Boolean) : String = value match {
            case Join(joinType, table, on : (Table => Value[Boolean]), body) =>
                val typeString = if(joinType == Inner) "" else joinType.toString.toUpperCase + " "
                "\n" + typeString + "JOIN " +
                "\"" + tableName(table) + "\" " + rowName(tableName(table), table.getRow.id) +
                (on(table) match { case Bool(true) => ""; case v => " ON " + apply(v) }) +
                apply(body.asInstanceOf[Table => Value[_]].apply(table))
            case Where(condition, result) => "\n" + (if(whereAnd) "AND" else "WHERE") + " " + apply(condition) + " " + apply(result)(true)
            case Scalar(column) => rowName(tableName(column.table), column.row.id) + ".\"" + column.getClass.getName.split('$').reverse.head + "\""
            case Select2(v1, v2) => "\nSELECT " + apply(v1) + ", " + apply(v2)
            case Select3(v1, v2, v3) => "\nSELECT " + apply(v1) + ", " + apply(v2) + ", " + apply(v3)
            case Select4(v1, v2, v3, v4) => "\nSELECT " + apply(v1) + ", " + apply(v2) + ", " + apply(v3) + ", " + apply(v4)

            case Equals(v1, v2) => "(" + apply(v1) + " = " + apply(v2) + ")"
            case NotEquals(v1, v2) => "(" + apply(v1) + " <> " + apply(v2) + ")"
            case Greater(v1, v2) => "(" + apply(v1) + " > " + apply(v2) + ")"
            case Less(v1, v2) => "(" + apply(v1) + " < " + apply(v2) + ")"
            case GreaterEquals(v1, v2) => "(" + apply(v1) + " >= " + apply(v2) + ")"
            case LessEquals(v1, v2) => "(" + apply(v1) + " <= " + apply(v2) + ")"

            case And(Bool(true), v2) => apply(v2)
            case And(v1, v2) => "(" + apply(v1) + " AND " + apply(v2) + ")"
            case Or(v1, v2) => "(" + apply(v1) + " OR " + apply(v2) + ")"

            case Int32(value : Int) => value.toString
            case Int64(value : Long) => value.toString
            case Bool(value : Boolean) => value.toString
            case Null() => "null"
        }
    }

    def From[T <: Table](table : Table.Row => T) = new ForSyntax.ForFrom[T](table)
    def Join[T <: Table](table : Table.Row => T) = new ForSyntax.ForJoin[T](table, _ => Value.Bool(value = true), JoinType.Inner)
    def LeftJoin[T <: Table](table : Table.Row => T) = new ForSyntax.ForJoin[T](table, _ => Value.Bool(value = true), JoinType.Left)
    def RightJoin[T <: Table](table : Table.Row => T) = new ForSyntax.ForJoin[T](table, _ => Value.Bool(value = true), JoinType.Right)
    def FullJoin[T <: Table](table : Table.Row => T) = new ForSyntax.ForJoin[T](table, _ => Value.Bool(value = true), JoinType.Full)
    def CrossJoin[T <: Table](table : Table.Row => T) = new ForSyntax.ForJoin[T](table, _ => Value.Bool(value = true), JoinType.Cross)

    def Join[T <: Table, V](column : Table.RowForeignKey[_, T, V]) : ForSyntax.ForJoin[T] = Join(column.foreign) on { f : T => Value.Equals(column.key(f), column) }
    def LeftJoin[T <: Table, V](column : Table.RowForeignKey[_, T, V]) : ForSyntax.ForJoin[T] = LeftJoin(column.foreign) on { f : T => Value.Equals(column.key(f), column) }
    def RightJoin[T <: Table, V](column : Table.RowForeignKey[_, T, V]) : ForSyntax.ForJoin[T] = RightJoin(column.foreign) on { f : T => Value.Equals(column.key(f), column) }
    def FullJoin[T <: Table, V](column : Table.RowForeignKey[_, T, V]) : ForSyntax.ForJoin[T] = FullJoin(column.foreign) on { f : T => Value.Equals(column.key(f), column) }
    def CrossJoin[T <: Table, V](column : Table.RowForeignKey[_, T, V]) : ForSyntax.ForJoin[T] = CrossJoin(column.foreign) on { f : T => Value.Equals(column.key(f), column) }

    def Select[V1](v1 : Value[V1]) : Value[V1] = v1
    def Select[V1, V2](v1 : Value[V1], v2 : Value[V2]) : Value[(V1, V2)] = Value.Select2(v1, v2)

    trait Session {
        def first[V](query : Query[V]) : Option[V]
        def list[V](query : Query[V]) : List[V]
        def foreach[V](query : Query[V])(body : V => Unit) : Unit
    }


    def main(arguments : Array[String]) {

        case class Note(row : Table.Row) extends Table(row) {
            object id extends Column[Long]
            object text extends Column[String]
        }

        case class Person(row : Table.Row) extends Table(row) {
            object id extends Column[Long]
            object spouseId extends ForeignKey[Person, Long](Person, _.id)
            object noteId extends ForeignKey[Note, Long](Note, _.id)
            object name extends Column[String]
            object age extends Column[Int]
        }

        case class Marriage(id1 : Long, id2 : Long)

        val query = for {
            person1 <- From(Person)
            person2 <- Join(person1.spouseId) on { p => p.age >= 18 && p.age < 30 }
            note <- LeftJoin(person2.noteId)
            if note.id !== person1.id
            if person1.id === person2.id
        } yield Select(person1.spouseId, person2.spouseId)

        val session = null : Session

        //val marriages = session.list(query)

        println(new Printer().apply(query))

    }
}
