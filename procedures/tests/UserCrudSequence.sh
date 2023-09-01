# back up original users
a5cli R user -o tmp/restore_users.json
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
a5cli C user procedures/tests/sample_data/user.json
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
a5cli C user procedures/tests/sample_data/users.json
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
a5cli R user name=test -o tmp/user.json
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
a5cli R user name=test -o tmp/user.csv -f csv
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
a5cli R user -o tmp/users.json
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
a5cli R user -o tmp/users.csv -f csv
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
a5cli U user name=test -u password=new -o tmp/user_upd.json
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
a5cli U user -u password=new -o tmp/users_upd.json
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
a5cli D user name=test -o tmp/user_del.json
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
a5cli D user -o tmp/users_del.json
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
# restore original users
a5cli C user tmp/restore_users.json
if [[ "$?" != "0" ]]
then echo "Test failed"
     exit 1
fi
echo "Test passed"
exit 0