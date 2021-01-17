import csv
import sys
import re
import itertools

DB_DIR = "files/"
META_FILE = "files/metadata.txt"
AGGREGATE = ["min", "max", "sum", "avg", "count"]
RELATE_OPS = ["<=", ">=", "<", ">", "="]
db_schema = {}
CONST = "INTCONST"

def raise_error(string):
    print(string)
    exit(0)

def AssertCond(condition, string):
    if condition:
        print(string)
        exit(0)

def isint(s):
    try:
        _ = int(s)
        return True
    except:
        return False

def ReadDbSchema():
    fp = open(META_FILE, "r")
    schema_contents = fp.readlines()
    cur_table = None
    for line in schema_contents:
        line = line.strip()
        if line == "<begin_table>":
            cur_table = None
        elif line == "<end_table>":
            continue
        elif cur_table is None:
            cur_table = line.lower()
            db_schema[cur_table] = []
        else:
            db_schema[cur_table] += [line.lower()]

def ReadCsv(file):
    with open(file) as fp:
        csv_reader = csv.reader(fp, delimiter=',')
        line_count = 0
        csv_reader = list(csv_reader)
        for idx,row in enumerate(csv_reader):
            for col_idx,val in enumerate(row):
                c = csv_reader[idx][col_idx]
                csv_reader[idx][col_idx] = int(c)
        return csv_reader

def IsAnd(s):
    if "and" in s:
        return True
    return False

def IsOr(s):
    if "or" in s:
        return True
    return False

def IsRelOp(op):
    flag = True
    rel_op = ''
    for opr in RELATE_OPS:
        if opr in op:
            flag = False
            rel_op = opr
            break

    AssertCond(flag, "invalid relational op: '{}'".format(op))
    AssertCond(op.count(rel_op) != 1, "invalid count of relational op: '{}'".format(op))
    left, right = op.split(rel_op)
    left, right = left.strip(), right.strip()
    return left, right, rel_op

def CheckRelop(cols, i, op):
    if op == ">":
        return cols[0][i] > cols[1][i]
    if op == "<":
        return cols[0][i] < cols[1][i]
    if op == ">=":
        return cols[0][i] >= cols[1][i]
    if op == "<=":
        return cols[0][i] <= cols[1][i]
    if op == "=":
        return cols[0][i] == cols[1][i]
    return True

def HandleRelop(op, take_cols, cols, cond_idx):
    col_len = len(cols[0])
    for i in range(col_len):
        if CheckRelop(cols, i, op):
                continue
        take_cols[i][cond_idx] = False
    
    return take_cols
    
def OutputTable(output_tables, output_cols, cols_needed, output_cond, cond_op):
    tables_needed = [[] for _ in output_tables]
    cross_join_idx = {}
    cnt = 0
    for idx, table in enumerate(output_tables):
        file_name = DB_DIR + table + '.csv'
        get_cols = ReadCsv(file_name)
        for i,row in enumerate(get_cols):
            rows = []
            for col_name in cols_needed[table]:
                col_idx = db_schema[table].index(col_name)
                rows.append(row[col_idx])
            tables_needed[idx].append(rows)   

        for i, cname in enumerate(cols_needed[table]):
            if table in cross_join_idx.keys():
                cross_join_idx[table].update({cname: cnt + i})
            else:
                cross_join_idx[table] = {cname: cnt + i}
        cnt += len(cols_needed[table])

    inter_table = [[i for tup in r for i in tup] for r in itertools.product(*tables_needed)]
    # for t in inter_table:
    #     print(t)
    final_table = []
    if output_cond != []:
        take_cols = [[True for _ in range(len(output_cond))] for _ in range(len(inter_table))]
        for cond_idx, cond in enumerate(output_cond):
            cols = []
            op = cond[0]
            for tab_name, col_name in cond[1:]:
                if tab_name != CONST:
                    col_idx = cross_join_idx[tab_name][col_name]
                    col = []
                    for row in inter_table:
                        col += [row[col_idx]]
                    cols.append(col)
                else:
                    cols.append([int(col_name)]*len(inter_table))
        
            take_cols = HandleRelop(op, take_cols, cols, cond_idx)

        if cond_op == "and":
            for i in range(len(inter_table)):
                take_cols[i][0] = take_cols[i][0] and take_cols[i][1]
        if cond_op == "or":
            for i in range(len(inter_table)):
                take_cols[i][0] = take_cols[i][0] or take_cols[i][1]

         
        for i,row in enumerate(inter_table):
            if not take_cols[i][0]:
                continue
            choose_cols = []
            for j, (t_name, c_name, _) in enumerate(output_cols):
                idx = cross_join_idx[t_name][c_name]
                choose_cols += [inter_table[i][idx]]
            final_table.append(choose_cols)           
        for t in final_table:
            print(t)
    
    # print(cross_join_idx)
    if output_cols[0][2]:
        for idx, (t_name, c_name, aggr) in enumerate(output_cols):
            cols = []
            for row_idx, row in enumerate(inter_table):
                cols += [row[idx]]
            if aggr == "min":
                final_table.append(min(cols))
            elif aggr == "sum":
                final_table.append(sum(cols))
            elif aggr == "average":
                final_table.append(sum(cols)/len(cols))
            elif aggr == "max":
                final_table.append(max(cols))
            elif aggr == "count":
                final_table.append(len(cols))
            elif aggr == "distinct":
                n = len(t_name)
                cols = [[] for _ in range(len(inter_table))]    
                for row_idx, row in enumerate(inter_table):
                    for j in range(n):
                        col_idx = cross_join_idx[t_name[j]][c_name[j]]
                        cols[row_idx] += [row[col_idx]]
                for row_idx, row in enumerate(inter_table):
                    if row not in final_table:
                        final_table.append(row)
            else:
                raise_error("Invalid aggregrate function")
        print(final_table)
def CondParser(cond_list, tables_list):
    output_cond = []
    cond_op = None
    if cond_list == []:
        return output_cond, cond_op
    
    cond_list = " ".join(cond_list)
    if IsAnd(cond_list):
        cond_list, cond_op = cond_list.split(" and "), "and"
    elif IsOr(cond_list):
        cond_list, cond_op = cond_list.split(" or "), "or"
    else:
        cond_list = [cond_list]
    # print(cond_list)
    for cond in cond_list:
        left, right, rel_op = IsRelOp(cond)
        expr_list = [left, right]
        cond_parse = [rel_op]
        for expr in expr_list:
            if isint(expr):
                cond_parse.append([CONST, expr])
            else:
                col_name = expr
                tnames = []
                for t in tables_list:
                    if col_name in db_schema[t]:
                        tnames += [t]
                AssertCond(tnames == [], "Unkown Attribute in where: '{}'".format(expr))
                cond_parse.append([tnames[0], col_name])

        output_cond.append(cond_parse)
    # print(output_cond, cond_op)
    return output_cond, cond_op
    
def TableParser(tables_list):
    output_tables = []
    # tables_list = "".join(tables_list).split(',')
    for table in tables_list:
        AssertCond(table not in db_schema.keys(), "Table name '{}'absent in schema".format(table))
        output_tables += [table]

    return output_tables

def _any(iterable):
    for itr in iterable:
        if itr:
            return True
    return False

def _all(iterable):
    for itr in iterable:
        if not itr:
            return False
    return True  

def ColoumnParser(tables_list, cols_list):
    output_cols = []
    # print(cols_list)
    for col in cols_list:
        # print(col)
        regmatch = re.match("(.+)\((.+)\)", col)
        if regmatch:
            aggr_func, col = regmatch.groups()
        else:
            aggr_func = None
        # print(aggr_func, col)
        tname = []
        if col != "*" and aggr_func != "distinct":
            for t in tables_list:
                if col in db_schema[t]:
                    tname += [t]
            AssertCond(len(tname) > 1, "Column Name not unique")
            AssertCond(tname == [], "Column Name doesn't exist in schema")
            output_cols += [(tname, [col], aggr_func)]
        elif col != "*":
            col = col.split(',')
            for t in tables_list:
                for coloumn in col:
                    if coloumn in db_schema[t]:
                        tname += [t]
            # print(col,tname)
            AssertCond(len(tname) != len(col), "Some column names don't exist in the schema")
            output_cols += [(tname, col, aggr_func)]
        else:
            AssertCond(aggr_func != None, "Aggregrate function can't be used with *")
            for t in tables_list:
                for c in db_schema[t]:
                    output_cols += [([t], [c], aggr_func)]
    
    aggr_list = []
    for _, _, aggr in output_cols:
        aggr_list += [aggr]
    AssertCond(_all(aggr_list)^_any(aggr_list), "Aggregrated coloumns can't be used with non-aggregrate ones")
    def is_distinct(iterable):
        for itr in iterable:
            if itr == "distinct":
                return True
        return False
    AssertCond(is_distinct(aggr_list) and len(aggr_list)!=1, "Distinct keyword can't be used with other coloumns")
    return output_cols

def QueryParser(q):
    q = q.lower().split()
    if q == []:
        raise_error("Empty Query")
    elif q[0] != "select":
        raise_error("Query should start with select clause")

    select_index = []
    from_index = []
    where_index = []
    groupby_index = []
    orderby_index = []
    n = len(q)
    # print(q)
    index = 0
    while index < n:
        val = q[index]
        if val == "from":
            from_index += [index]
        elif val == "select":
            select_index += [index]
        elif val == "where":
            where_index += [index]
        elif val == "group":
            next_index = index + 1
            if next_index < n:
                AssertCond(q[next_index] != "by", "Query is Invalid")
                groupby_index += [index, index + 1]
                index += 2
                continue
            else:
                raise_error("by missing after group")
        elif val == "order":
            next_index = index + 1
            if next_index < n:
                AssertCond(q[next_index] != "by", "Query is Invalid")
                orderby_index += [index, index + 1]
                index += 2
                continue
            else:
                raise_error("by missing after order")
        index += 1

    cond1 = len(select_index) != 1 or len(from_index) != 1 or len(where_index) > 1
    cond2 = len(groupby_index) > 2 or len(orderby_index) > 2
    AssertCond(cond1 or cond2, "Query is Invalid")
    tables_list = []
    cond_list = []
    groupby_list = []
    orderby_list = []
    if where_index:
        cond = where_index[0] < from_index[0]
        AssertCond(cond, "Query is Invalid")
        tables_list += q[from_index[0] + 1: where_index[0]]
        if groupby_index:
            cond = cond or where_index[0] > groupby_index[0]
            AssertCond(cond, "Query is Invalid")
            cond_list += q[where_index[0] + 1: groupby_index[0]]
            if orderby_index:
                cond = cond or groupby_index[0] > orderby_index[0]
                AssertCond(cond, "Query is Invalid")
                groupby_list += q[groupby_index[0] + 1: orderby_index[0]]
                orderby_list += q[orderby_index[0] + 1:]
            else:
                groupby_list += q[groupby_index[0] + 1:]
        else:
            cond_list += q[where_index[0] + 1:]
    else:
        if groupby_index:
            cond = groupby_index[0] < from_index[0]
            AssertCond(cond, "Query is Invalid")
            tables_list += q[from_index[0] + 1: groupby_index[0]]
            if orderby_index:
                cond = cond or groupby_index[0] > orderby_index[0]
                AssertCond(cond, "Query is Invalid")
                groupby_list += q[groupby_index[1] + 1: orderby_index[0]]
                orderby_list += q[orderby_index[1] + 1:]
            else:
                groupby_list += q[groupby_index[1] + 1:]
        else:
            if orderby_index:
                cond = orderby_index[0] < from_index[0]
                AssertCond(cond, "Query is Invalid")
                tables_list += q[from_index[0] + 1: orderby_index[0]]
                orderby_list += q[orderby_index[1] + 1:]
            else:
                tables_list += q[from_index[0] + 1:]
    
    cols_list = q[select_index[0] + 1:from_index[0]]
    # print(tables_list, cond_list, groupby_list, orderby_list, cols_list)
    return tables_list, cols_list, cond_list, groupby_list, orderby_list    
    
def pre_process_query(q):
    out_q = []
    q = q.strip()
    if q[-1] == ';':
        q = q[:len(q) - 1]
    q = q.lower().split()
    dis_idx = -1
    from_idx = -1
    for i,keyword in enumerate(q):
        if keyword == "distinct":
            dis_idx = i
        if keyword == "from":
            from_idx = i
    cols = '('
    from_idx = -1 if dis_idx == -1 else from_idx
    for i in range(dis_idx + 1, from_idx):
        cols += q[i]
    cols += ')'
    i = 0
    n = len(q)
    while i < n:
        if q[i] == "distinct":
            out_q.append(q[i] + cols)
            i = from_idx
            continue
        out_q.append(q[i])
        i += 1
    return " ".join(out_q)

def main():
    q = ''
    if len(sys.argv) == 2:
        q = sys.argv[1]
        q = pre_process_query(q)
        print(q)
    else:
        raise_error("Invalid Query format")
    ReadDbSchema()
    p = QueryParser(q)
    tables_list = "".join(p[0]).split(',')
    output_tables = TableParser(tables_list)
    output_cols = ColoumnParser(tables_list, p[1])
    print(output_cols)
    output_cond, cond_op = CondParser( p[2], tables_list)
    print(output_cond)
    cols_needed = {t : set() for t in output_tables}
    for table, col, aggr in output_cols:
        for i in range(len(table)):
            cols_needed[table[i]].add(col[i])
    
    for cond in output_cond:
        cond = cond[1:]
        for table, val in cond:
            if table != CONST:
                cols_needed[table].add(val)

    OutputTable(output_tables, output_cols, cols_needed, output_cond, cond_op)
main()