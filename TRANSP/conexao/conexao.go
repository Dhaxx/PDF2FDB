package conexao

import (
	"database/sql"
	"fmt"

	_ "github.com/nakagami/firebirdsql"
)

func Conexao() (*sql.DB, error) {
	Cnx, err := sql.Open("firebirdsql", ("FSCSCPI8:scpi@localhost:3050/C:\\Fiorilli\\BANCOS\\SCPI\\DADOS\\PREFEITURA\\ARQ2024\\SCPI2024.FDB"))

	if err != nil {
		panic(err)
	}

	err = Cnx.Ping()
	if err != nil {
		panic(err)
	}
	fmt.Printf("CONEXÃO ESTABELECIDA: 2024\n")

	return Cnx, err
}