import csv
import sys
import re
import itertools

TABLE_DIR = "files/"
METAFILE = "files/metadata.txt"
RELATIONAL_OPS = ["<=", ">=", "<", ">", "="]
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
    fp = open(METAFILE, "r")
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
    for opr in RELATIONAL_OPS:
        if opr in op:
            flag = False
            rel_op = opr
            break

    AssertCond(flag, "invalid relational op: '{}'".format(op))
    AssertCond(op.count(rel_op) != 1, "invalid count of relational op: " + op)
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
    
def OutputTable(output_tables, output_cols, cols_needed, output_cond, cond_op, groupby_dict, orderby_col, is_dist):
    tables_needed = [[] for _ in output_tables]
    cross_join_idx = {}
    count = 0
    for idx, table in enumerate(output_tables):
        file_name = TABLE_DIR + table + '.csv'
        get_cols = ReadCsv(file_name)
        for i,row in enumerate(get_cols):
            rows = []
            for col_name in cols_needed[table]:
                col_idx = db_schema[table].index(col_name)
                rows.append(row[col_idx])
            tables_needed[idx].append(rows)   

        cross_join_idx[table] = {c_name: count+i for i, c_name in enumerate(cols_needed[table])}
        count = count + len(cols_needed[table])

    # Cartesain product of the tables i.e from clause
    inter_table = [[i for tup in r for i in tup] for r in itertools.product(*tables_needed)]
    final_table = []
    clause = False

    # execute where clause
    if output_cond != []:
        clause = True
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
                n = len(t_name)
                for k in range(n):
                    idx = cross_join_idx[t_name[k]][c_name[k]]
                    choose_cols += [inter_table[i][idx]]
            final_table.append(choose_cols)           

    # execute group by clause
    if groupby_dict['groupby_table']:
        aggr_cols = groupby_dict['aggr_cols']
        seen = {}
        if clause:
            temp_table = final_table.copy()
        else:
            temp_table = inter_table.copy()
        clause = True
        final_table = []
        proj_col_idx = None
        def find_proj_idx(proj_col_idx):
            for j, (t_name, c_name, aggr) in enumerate(output_cols):
                for k, cname in enumerate(c_name):
                    if cname == groupby_dict['proj_col']:
                        proj_col_idx = j
                        break
                if proj_col_idx:
                    break
            return proj_col_idx

        proj_col_idx = find_proj_idx(proj_col_idx)
        for j, (t_name, c_name, aggr) in enumerate(output_cols):
            for k, cname in enumerate(c_name):
                rows = []
                if cname == groupby_dict['proj_col']:
                    continue
                for i, row in enumerate(temp_table):
                    val = row[proj_col_idx]  
                    if val not in seen.keys():
                        seen[val] = {} 
                    if cname not in seen[val].keys():
                        seen[val][cname] = []

                    seen[val][cname].append((row[j], aggr))

        def dict_helper():
            for key, val in seen.items():
                rows = []
                for key1, val1 in val.items():
                    temp = [itr for itr, _ in val1]
                    aggr = val1[0][1]
                    # print(key1,temp)
                    if len(rows) == proj_col_idx:
                        rows += [key]
                    if aggr == "min":
                        rows += [min(temp)]
                    elif aggr == "max":
                        rows += [max(temp)]
                    elif aggr == "sum":
                        rows += [sum(temp)]
                    elif aggr == "average":
                        rows += [sum(temp)/len(temp)]
                    elif aggr == "count":
                        rows += [len(temp)]
                    else:
                        raise_error("Aggregrate function is invalid")
                
                final_table.append(rows)
        
        dict_helper()
    
    aggr_list = [a for _, _, a in output_cols]
    # execute simple aggregrate functions
    if _all(aggr_list):
        if clause:
            temp_table = final_table.copy()
        else:
            temp_table = inter_table.copy()
        clause = True
        aggr_outs = []
        for idx, (t_name, c_name, aggr) in enumerate(output_cols):
            cols = []
            for row_idx, row in enumerate(temp_table):
                cols += [row[idx]]
            if aggr == "min":
                aggr_outs += [min(cols)]
            elif aggr == "sum":
                aggr_outs += [sum(cols)]
            elif aggr == "average":
                aggr_outs += [sum(cols)/len(cols)]
            elif aggr == "max":
                aggr_outs += [max(cols)]
            elif aggr == "count":
                aggr_outs += [len(cols)]
            else:
                raise_error("Invalid aggregrate function")
        final_table = [[]]
        final_table[0] = aggr_outs.copy()
    
    # execute distinct clause  
    if is_dist:
        if clause:
            temp_table = final_table.copy()
        else:
            temp_table = inter_table.copy()
        clause = True
        final_table = []
        for row_idx, row in enumerate(temp_table):
            rows = []
            for col_idx, col_val in enumerate(row):
                rows.append(col_val)
            if rows not in final_table:
                final_table.append(rows)

    # execute order by clause
    if orderby_col[0]:
        orderby_table = None
        col_order_idx = None
        if not clause:
            final_table = inter_table.copy()
        clause = True
        def find_orderby_idx(col_order_idx):
            for j, (t_name, c_name, aggr) in enumerate(output_cols):
                for k, cname in enumerate(c_name):
                    if cname == orderby_col[0]:
                        col_order_idx = j
                        break
                if col_order_idx:
                    break
            return col_order_idx

        col_order_idx = find_orderby_idx(col_order_idx)
        final_table = sorted(final_table, key = lambda x: x[col_order_idx])
        if orderby_col[1] == "desc":
            final_table = final_table[::-1]
        elif orderby_col[1] != "asc":
            raise_error("Invalid order by option: Use asc/desc")

    if not clause:
        final_table = inter_table.copy()
    # print(final_table)
    col_names = []
    for j, (t_name, c_name, aggr) in enumerate(output_cols):
        for k, cname in enumerate(c_name):
            if aggr:
                col_names.append(aggr + '(' + t_name[k] + '.' + cname + ')')
            else:
                col_names.append(t_name[k] + '.' + cname)
    # print(col_names)
    return final_table, col_names

def OrderByParser(orderby_col, output_cols):
    if orderby_col == []:
        return [None]
    f = True
    col_ord = orderby_col[0]
    regmatch = re.match("(.+)\((.+)\)", col_ord)
    AssertCond(len(orderby_col) == 1, "Missing asc/desc option in order by clause")
    if regmatch:
        aggr_func, col_ord = regmatch.groups()
    for i, (t_name, c_name, aggr) in enumerate(output_cols):
        col = c_name[0]
        if col == col_ord:
            f = False
            break
    AssertCond(f, "No projection column found corresponding to the given order by column")
    return [col_ord, orderby_col[1]]
    
def GroupByParser(groupby_list, tables_list, output_cols):
    if groupby_list == []:
        return None, None
    col_to_grp = groupby_list[1]
    flag = True
    groupby_table = None
    for table in tables_list:
        if col_to_grp in db_schema[table]:
            groupby_table = table
            break
    AssertCond(groupby_table == None, "No matching table for the given group by column")
    for j, (t_name, c_name, aggr) in enumerate(output_cols):
        col = c_name[0]
        if col_to_grp == col:
            flag = False
            break
    AssertCond(flag, "No projection column found corresponding to the given group by column")
    flag = True
    aggr_cols = []
    for j, (t_name, c_name, aggr) in enumerate(output_cols):
        col = c_name[0]
        if aggr:
            aggr_cols += [col]
    AssertCond(len(aggr_cols)==0, "No aggregrate function found for the group by clause")
    
    return groupby_table, aggr_cols

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
    return output_cond, cond_op
    
def TableParser(tables_list):
    output_tables = []
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
    for col in cols_list:
        regmatch = re.match("(.+)\((.+)\)", col)
        if regmatch:
            aggr_func, col = regmatch.groups()
        else:
            aggr_func = None
        tname = []
        if col != "*":
            for t in tables_list:
                if col in db_schema[t]:
                    tname += [t]
            AssertCond(len(tname) > 1, "Column Name not unique")
            AssertCond(tname == [], "Column Name doesn't exist in schema")
            output_cols += [(tname, [col], aggr_func)]
        else:
            AssertCond(aggr_func != None, "Aggregrate function can't be used with *")
            for t in tables_list:
                for c in db_schema[t]:
                    output_cols += [([t], [c], aggr_func)]
    
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
                orderby_list += q[orderby_index[1] + 1:]
            else:
                groupby_list += q[groupby_index[0] + 1:]
        else:
            if orderby_index:
                cond = cond or where_index[0] > orderby_index[0]
                AssertCond(cond, "Query is Invalid")
                cond_list += q[where_index[0] + 1: orderby_index[0]]
                orderby_list += q[orderby_index[1] + 1:]
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
                groupby_list += q[groupby_index[0] + 1: orderby_index[0]]
                orderby_list += q[orderby_index[1] + 1:]
            else:
                groupby_list += q[groupby_index[0] + 1:]
        else:
            if orderby_index:
                cond = orderby_index[0] < from_index[0]
                AssertCond(cond, "Query is Invalid")
                tables_list += q[from_index[0] + 1: orderby_index[0]]
                orderby_list += q[orderby_index[1] + 1:]
            else:
                tables_list += q[from_index[0] + 1:]
    
    cols_list = q[select_index[0] + 1:from_index[0]]
    return tables_list, cols_list, cond_list, groupby_list, orderby_list   
    
def pre_process_query(q):
    q = q.strip()
    if q[-1] == ';':
        q = q[:len(q) - 1]
    else:
        raise_error("Semicolon missing at end")
    
    q = q.lower().split()
    dis_idx = []
    for i,keyword in enumerate(q):
        if keyword == "distinct":
            dis_idx += [i]

    AssertCond(len(dis_idx) > 1, "Distinct keyword appears more than once") 
    if dis_idx != []:
        q = q[:dis_idx[0]] + q[dis_idx[0] + 1:]
    return " ".join(q), dis_idx

def get_output(final_table, col_names):
    print(",".join(col_names))
    for i, row in enumerate(final_table):
        print(*row, sep=', ')
def main():
    q = ''
    is_distinct = None
    if len(sys.argv) == 2:
        q = sys.argv[1]
        q, is_distinct = pre_process_query(q)
        # print(q)
    else:
        raise_error("Invalid Query format")
    ReadDbSchema()
    p = QueryParser(q)
    tables_list = "".join(p[0]).split(',')
    output_tables = TableParser(tables_list)
    cols_list = "".join(p[1]).split(',')
    output_cols = ColoumnParser(tables_list, cols_list)
    output_cond, cond_op = CondParser(p[2], tables_list)
    cols_needed = {t : [] for t in output_tables}
    for table, col, aggr in output_cols:
        for i in range(len(table)):
            cols_needed[table[i]].append(col[i])
    
    for cond in output_cond:
        cond = cond[1:]
        for table, val in cond:
            if table != CONST:
                if val in cols_needed[table]:
                    continue
                cols_needed[table].append(val)

    groupby_table, aggr_cols = GroupByParser(p[3], tables_list, output_cols)
    orderby_col = OrderByParser(p[4], output_cols)
    groupby_dict = {}
    groupby_dict['groupby_table'] = None
    groupby_dict['aggr_cols'] = None
    groupby_dict['proj_col'] = None
    if groupby_table:
        groupby_dict['groupby_table'] = groupby_table
        groupby_dict['aggr_cols'] = aggr_cols
        groupby_dict['proj_col'] = p[3][1]
    
    is_dist = (len(is_distinct) != 0)
    final_table, col_names =  OutputTable(output_tables, output_cols, cols_needed, output_cond, cond_op, groupby_dict, orderby_col, is_dist)
    get_output(final_table, col_names)
main()