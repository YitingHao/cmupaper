"""
A customized checker
"""

# Import necessary packages
import paper.database_wrapper as db_wrapper
import paper.functions as funcs
from paper.constants import *

from datetime import datetime
from datetime import timedelta
# from pytz import timezone
import psycopg2 as psy

"""
Test constants
"""
USERS = ["foo", "bar", "foofoo", "barbar", "foobar"]

VERBOSE = False
RUNTESTS = {}

def exit_test():
    # report_result()
    exit(1)

def error_message(func, msg, should_abort = True):
    RUNTESTS[func.__name__]["PASS"] = False
    print "[Error in %s]: %s" % (func.__name__, msg)
    if should_abort:
        exit_test()

def format_error(func, should_abort = True):
    error_message(func, "incorrect return value format", should_abort)

def status_error(func, should_abort = True):
    error_message(func, "failed unexpectedly", should_abort)

def db_wrapper_debug(func, argdict, verbose = VERBOSE):
    if verbose:
        msg = "[Test] %s(" % func.__name__
        for k,v in argdict.iteritems():
            msg += " %s = %s," % (str(k), str(v))
        msg += " )"
        print msg
    res = db_wrapper.call_db(func, argdict)
    if verbose:
        print "\treturn: %s" % str(res)
    return res

# invalid tests
def invalid_tests(tests):
	for func, args, name, errsign, errmsg in tests:
		try:
			status, res = db_wrapper_debug(func, args)
			if status != errsign:
				error_message(func, errmsg)
			RUNTESTS[func.__name__][name] = True
			print("  Success " + name + "-" + func.__name__)
		except TypeError:
			format_error(func)

# reset_db pg_class double check (for part of T.1 debug)
def reset_db_pg_class(conn, tables):
    try:
        cur = conn.cursor()
        for table in tables:
        	SQL = "SELECT relname, reltuples FROM pg_class WHERE relname = %s;"
        	data = (table, )
        	cur.execute(SQL, data)
        	res = cur.fetchall()
        	if len(res) != 1 or res[0][1] != 0:
        		return 1, None
        return 0, None
    except psy.DatabaseError, e:
        return 1, None


# Test reset_db ALL
print("Test T1: Reset database")
RUNTESTS[funcs.reset_db.__name__] = {}
RUNTESTS[funcs.reset_db.__name__]["PASS"] = True
# basic test
try:
	status, res = db_wrapper_debug(funcs.reset_db, {})
	if status != SUCCESS:
		status_error(funcs.reset_db)
	RUNTESTS[funcs.reset_db.__name__]["basic"] = True
	print("  Success basic")
except TypeError:
	format_error(funcs.reset_db)
# pg_class database check
try:
	status, res = db_wrapper_debug(reset_db_pg_class, {'tables':['users','papers','paper_text_idx','tagnames','likes','tags']})
	if status != SUCCESS:
		status_error(reset_db_pg_class)
	RUNTESTS[funcs.reset_db.__name__]["pg_class"] = True
	print("  Success pg_class")
except TypeError:
	format_error(reset_db_pg_class)


# Test signup basic
print("Test T2: Create user account")
RUNTESTS[funcs.signup.__name__] = {}
RUNTESTS[funcs.signup.__name__]["PASS"] = True
# basic test
try:
	for user in USERS:
		status, res = db_wrapper_debug(funcs.signup, {'uname':user, 'pwd':user})
		if status != SUCCESS:
			status_error(funcs.signup)
	RUNTESTS[funcs.signup.__name__]["basic"] = True
	print("  Success basic")
except TypeError:
	format_error(funcs.signup)


# Test login basic
print("Test T3: Login")
RUNTESTS[funcs.login.__name__] = {}
RUNTESTS[funcs.login.__name__]["PASS"] = True
# basic test
try:
	for user in USERS:
		status, res = db_wrapper_debug(funcs.login, {'uname':user, 'pwd':user})
		if status != SUCCESS:
			status_error(funcs.login)
	RUNTESTS[funcs.login.__name__]["basic"] = True
	print("  Success basic")
	status, res = db_wrapper_debug(funcs.login, {'uname':USERS[0], 'pwd':USERS[1]})
	if status == SUCCESS:
		error_message(funcs.login, "password is not matched but still return login success")
	RUNTESTS[funcs.login.__name__]["pwd_not_match"] = True
except TypeError:
	format_error(funcs.login)

# test login (T.3 Login)

# test add_new_paper (T.4 upload a paper with tags)

# test delete_paper (T.5 Delete a paper)

# test get_timeline (T.6 User timeline)

# test get_timeline_all (T.7 Global timeline)

# test get_papers_by_keyword (T.8 Search for papers)

# test get_papers_by_tag (T.9 Search by a tag)

# test get_paper_tags (T.10 Get tags of a paper)

# test like_paper & unlike_paper (T.11 Like/Unlike a paper)

# test get_likes (T.12 Count likes)

# test get_papers_by_liked (T.13 List favourite papers of a user)

# test get_most_popular_papers (T.14 List most popular papers)

# test get_recommend_papers (T.15 Recommned papers based on likes)

# test get_number_papers_user & get_number_liked_user & get_number_tags_user (T.16 User statistics)

# test get_most_active_users & get_most_popular_tags & get_most_popular_tag_pairs (T.17 Global statistic)


# Invalid values
usersInvalid = "usersInvalidusersInvalidusersInvalidusersInvalidusersInvalid"
pwdInvalid = "pwdInvalidpwdInvalidpwdInvalidpwdInvalid"

# Invalid Tests (func, arguments, test name, error message)
invalid_func_tests = [
	(funcs.signup, {'uname':usersInvalid, 'pwd':"validpassword"}, "invalid_username", 2, "invalid username test fails"), #signup: invalid username
	(funcs.signup, {'uname':"validusername", 'pwd':pwdInvalid}, "invalid_pwd", 2, "invalid password test fails"), #signup: invalid password
	(funcs.signup, {'uname':usersInvalid, 'pwd':pwdInvalid}, "invalid_username_and_pwd", 2, "invalid username & password test fails"), #signup: both invalid inputs
	(funcs.signup, {'uname':USERS[0], 'pwd':USERS[0]}, "exist_user", 1, "exist user test fail"), #signup: exist user
	# (),
]

invalid_tests(invalid_func_tests)

print(RUNTESTS)

# Reset database
db_wrapper_debug(funcs.reset_db, {})

