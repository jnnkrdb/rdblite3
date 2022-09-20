// This package is wrapper for sqlite3 db connections. To use it with your own structs,
// you have to add an column-tag to your structs and an ID field.
//
// This package is currently in a BETA-state.
package rdblite3

import (
	"database/sql"
	"reflect"

	"github.com/jnnkrdb/jlog"

	_ "github.com/mattn/go-sqlite3"
)

// sqlite3 struct to group informations, json ready
type SQLite3 struct {
	db          *sql.DB
	Destination string `json:"destination"`
	Driver      string
}

// connect to the sqlite database file
func (sql3 *SQLite3) Connect() {

	jlog.Log.Println("connecting to database-file at", sql3.Destination)

	sql3.Driver = "sqlite3"

	if sql3.Destination != "" {

		if tmpDB, err := sql.Open(sql3.Driver, sql3.Destination); err != nil {

			jlog.Log.Println("connecting to database-file at", sql3.Destination)

			jlog.PrintObject(sql3, tmpDB, err)

		} else {

			sql3.db = tmpDB

			sql3.CheckConnection()
		}
	}
}

// check connection to dbfile
func (sql3 *SQLite3) CheckConnection() error {

	jlog.Log.Println("checking connection")

	if err := sql3.db.Ping(); err != nil {

		jlog.PrintObject(sql3, err)

		return err

	} else {

		jlog.Log.Println("connection established:", sql3.Destination)

		return nil
	}
}

// get the db pointer from the sqlite3 pkg
func (sql3 SQLite3) DB() *sql.DB {

	return sql3.db
}

// disconnect from the currently connected file
func (sql3 SQLite3) Disconnect() error {

	jlog.Log.Println("connection closed:", sql3.Destination)

	return sql3.db.Close()
}

// this function generates a "select" statement, where the estimated object
// is exactly one object, not from a list. The function requires the object-
// struct to have an "ID" field.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `objPointer` : interface{} | *struct > pointer to the object, is used to get the struct-informations
//   - `obj` : interface{} | struct > the same object, but not as a pointer, where the data should be stored
func (sql3 SQLite3) SelectObject(tblName string, objPointer, obj interface{}) error {

	sql := "SELECT "

	for i := 0; i < reflect.ValueOf(objPointer).Elem().NumField()-1; i++ {

		sql += reflect.TypeOf(obj).Field(i).Tag.Get("column") + ", "
	}

	sql += reflect.TypeOf(obj).Field(reflect.ValueOf(objPointer).Elem().NumField()-1).Tag.Get("column") + " FROM " + tblName + " WHERE id=?;"

	args := make([]interface{}, reflect.ValueOf(objPointer).Elem().NumField())

	row := sql3.DB().QueryRow(sql, reflect.ValueOf(objPointer).Elem().FieldByName("ID").Int())

	for i := 0; i < len(args); i++ {

		args[i] = reflect.ValueOf(objPointer).Elem().Field(i).Addr().Interface()
	}

	if err := row.Scan(args...); err != nil {

		jlog.PrintObject(sql3, objPointer, obj, sql, args, row, err)

		return err
	}

	jlog.Log.Println("selected | collected rows: ", 1)

	return nil
}

// this function generates a "select" statement, where the estimated object
// is a list of objects. The function does not require the object struct to have an "ID" field.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `obj` : interface{} | struct > the same object, but not as a pointer, is used to get the struct-informations
func (sql3 SQLite3) SelectObjects(tblName string, obj interface{}) error {

	sql := "SELECT * FROM " + tblName + ";"

	if rows, err := sql3.DB().Query(sql); err != nil {

		jlog.PrintObject(sql3, obj, sql, rows, err)

		return err

	} else {

		destv := reflect.ValueOf(obj).Elem()

		args := make([]interface{}, destv.Type().Elem().NumField())

		var rowscount int = 0

		for rows.Next() {

			rowp := reflect.New(destv.Type().Elem())

			rowv := rowp.Elem()

			for i := 0; i < rowv.NumField(); i++ {

				args[i] = rowv.Field(i).Addr().Interface()
			}

			if err := rows.Scan(args...); err != nil {

				jlog.PrintObject(sql3, obj, sql, rows, destv, args, rowscount, rowp, rowv, err)

				return err
			}

			destv.Set(reflect.Append(destv, rowv))

			rowscount++
		}

		jlog.Log.Println("selected | collected rows: ", rowscount)

		return nil
	}
}

// this function generates an "insert" statement, where the given struct
// is inserted into the given table. The function requires the object struct to have an "ID" field,
// so the struct receives the new id.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `objPointer` : interface{} | *struct > pointer to the object, is used to get the struct-informations
//   - `obj` : interface{} | struct > the same object, but not as a pointer, is used to get the struct-informations
func (sql3 SQLite3) InsertObject(tblName string, objPointer, obj interface{}) error {

	sql := "INSERT INTO " + tblName + " ( "

	for i := 1; i < reflect.ValueOf(objPointer).Elem().NumField(); i++ {

		sql += reflect.TypeOf(obj).Field(i).Tag.Get("column")

		if i != reflect.ValueOf(objPointer).Elem().NumField()-1 {

			sql += ", "
		}
	}

	sql += " ) VALUES ( "

	for i := 1; i < reflect.ValueOf(objPointer).Elem().NumField(); i++ {

		sql += "?"

		if i != reflect.ValueOf(objPointer).Elem().NumField()-1 {

			sql += ", "
		}
	}

	sql += " );"

	if statement, err := sql3.DB().Prepare(sql); err != nil {

		jlog.PrintObject(sql3, objPointer, obj, sql, statement, err)

		return err

	} else {

		args := make([]interface{}, reflect.ValueOf(objPointer).Elem().NumField()-1)

		for i := 0; i < len(args); i++ {

			args[i] = reflect.ValueOf(objPointer).Elem().Field(i + 1).Interface()
		}

		if result, err := statement.Exec(args...); err != nil {

			jlog.PrintObject(sql3, objPointer, obj, sql, statement, args, result, err)

			return err

		} else {

			if id, err := result.LastInsertId(); err != nil {

				jlog.PrintObject(sql3, objPointer, obj, sql, statement, args, result, id, err)

				return err

			} else {

				reflect.ValueOf(objPointer).Elem().FieldByName("ID").SetInt(id)

				jlog.Log.Println("inserted | new id: ", id)

				return nil
			}
		}
	}
}

// this function generates an "update" statement, where the given struct updates the values
// in the given table. The function requires the object struct to have an "ID" field,
// so the table receives the new values.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `objPointer` : interface{} | *struct > pointer to the object, is used to store the values
//   - `obj` : interface{} | struct > the same object, but not as a pointer, is used to get the struct-informations
func (sql3 SQLite3) UpdateObject(tblName string, objPointer, obj interface{}) error {

	sql := "UPDATE " + tblName + " SET "

	for i := 1; i < reflect.ValueOf(objPointer).Elem().NumField(); i++ {

		sql += reflect.TypeOf(obj).Field(i).Tag.Get("column") + "=?"

		if i != reflect.ValueOf(objPointer).Elem().NumField()-1 {

			sql += ", "
		}
	}

	sql += " WHERE id=?;"

	if statement, err := sql3.DB().Prepare(sql); err != nil {

		jlog.PrintObject(sql3, objPointer, obj, sql, statement, err)

		return err

	} else {

		args := make([]interface{}, reflect.ValueOf(objPointer).Elem().NumField())

		for i := 1; i < len(args); i++ {

			args[i-1] = reflect.ValueOf(objPointer).Elem().Field(i).Interface()
		}

		args[len(args)-1] = reflect.ValueOf(objPointer).Elem().Field(0).Interface()

		if result, err := statement.Exec(args...); err != nil {

			jlog.PrintObject(sql3, objPointer, obj, sql, statement, args, result, err)

			return err

		} else {

			if rowsaffected, err := result.RowsAffected(); err != nil {

				jlog.PrintObject(sql3, objPointer, obj, sql, statement, args, result, rowsaffected, err)

				return err

			} else {

				jlog.Log.Println("updated | updated rows: ", rowsaffected)

				return nil
			}
		}
	}
}

// this function generates an "delete" statement, where the given struct is used to delete the object
// from the given table. The function requires the object struct to have an "ID" field with a valid value.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `obj` : interface{} | struct > the object with an ID-field and an valid value
func (sql3 SQLite3) DeleteObject(tblName string, obj interface{}) error {

	sql := "DELETE FROM " + tblName + " WHERE id=?;"

	if statement, err := sql3.DB().Prepare(sql); err != nil {

		jlog.PrintObject(sql3, obj, sql, statement, err)

		return err

	} else {

		if result, err := statement.Exec(reflect.ValueOf(obj).Elem().FieldByName("ID").Interface()); err != nil {

			jlog.PrintObject(sql3, obj, sql, statement, result, err)

			return err

		} else {

			if rowsaffected, err := result.RowsAffected(); err != nil {

				jlog.PrintObject(sql3, obj, sql, statement, result, rowsaffected, err)

				return err

			} else {

				jlog.Log.Println("deleted | updated rows: ", rowsaffected)

				return nil
			}
		}
	}
}
