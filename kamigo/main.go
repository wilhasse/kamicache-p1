package main

import (
	"log"
	"net"
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

type colX struct{
        colNames []string
}

func (v *colX) Enter(in ast.Node) (ast.Node, bool) {
        if name, ok := in.(*ast.ColumnName); ok {
                v.colNames = append(v.colNames, name.Name.O)
        }
        return in, false
}

func (v *colX) Leave(in ast.Node) (ast.Node, bool) {
        return in, true
}
func extract(rootNode *ast.StmtNode) []string {
        v := &colX{}
        (*rootNode).Accept(v)
        return v.colNames
}
func parse(sql string) (*ast.StmtNode, error) {
        p := parser.New()

        stmtNodes, _, err := p.ParseSQL(sql)
        if err != nil {
                return nil, err
        }

        return &stmtNodes[0], nil
}

type ResultHandler struct{}

func (h ResultHandler) UseDB(dbName string) error {
	return nil
}

func (h ResultHandler) HandleQuery(query string) (*mysql.Result, error) {

	astNode, err := parse(query)
	result := extract(astNode)

	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
	}
	fmt.Printf("astNode: %v\n", result)

	var r *mysql.Resultset
	r, _ = mysql.BuildSimpleResultset([]string{"a", "b"}, [][]interface{}{
		{result[0],result[1]},
	}, false)

	return &mysql.Result{
		Status:       0,
		Warnings:     0,
		InsertId:     0,
		AffectedRows: 0,
		Resultset:    r,
	}, nil
}

func (h ResultHandler) HandleOtherCommand(cmd byte, data []byte) error {
    return nil
}

func (h ResultHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}

func (h ResultHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	return 0, 0, nil, nil
}

func (h ResultHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return h.HandleQuery(query)
}

func (h ResultHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func main() {
	l, err := net.Listen("tcp", "127.0.0.1:4000")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Print(err)
			continue
		}

		go handleConnection(c)
	}
}

func handleConnection(c net.Conn) {
	defer c.Close()

	conn, err := server.NewConn(c, "root", "", ResultHandler{})
	if err != nil {
		log.Print(err)
		return
	}

	for {
		if err := conn.HandleCommand(); err != nil {
			log.Print(err)
			return
		}
	}
}
