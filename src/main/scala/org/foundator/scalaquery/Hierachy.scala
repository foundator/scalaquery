package org.foundator.scalaquery

object Hierachy {

    // This is a state machine DAG that says "what can follow this syntactically"

    class With // -> With, From, Update, InsertInto, DeleteFrom

    class InsertInto // -> From
    class From // -> Join
    class Update // -> When
    class DeleteFrom // -> When

    class When // -> When

    class Join // -> Join, Where

    class Where // -> Where, GroupBy, Select

    class GroupBy // -> Having

    class Having // -> Having, Select

    class Select // -> Limit

    class Limit

}
