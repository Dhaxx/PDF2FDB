package main

import (
	"fmt"
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/nakagami/firebirdsql"
)

func main() {
	TipoArquivos()
	cnxFdb, err := ConexaoFirebird()
	if err != nil {
		Error(err)
	}

	cnxSql, err := ConexaoSqlServer()
	if err != nil {
		Error(err)
	}

	aux1, err := cnxFdb.Query(`SELECT TITULO, CODIGO FROM MODELO_RTF mr `)
	if err != nil {
		Error(err)
	}

	tipos := make(map[string]int)
	for aux1.Next() {
		var titulo string
		var codigo int
		err := aux1.Scan(&titulo, &codigo)
		if err != nil {
			Error(err)
		}
		tipos[titulo] = codigo
	}

	rows, err := cnxSql.Query(`select
									ROW_NUMBER() over (partition by b.sigla + '-' + cast(convit as varchar)+ '/' + cast(anoc as varchar) order by idarquivo) codigo,
									--a.idarquivo,
									c.descricao,
									convert(varchar(10),a.dtinclusao,23) dtinclusao,
									a.arquivo,
									b.sigla + '-' + cast(convit as varchar)+ '/' + cast(anoc as varchar) mascmod
								from
									mat.MXT04900 a
								join mat.MCT67600 b on
									a.idAgenda = b.IdAgenda
								join mat.MXT05000 c on
									a.idTipoArquivo = c.idTipoArquivo
								where anoc < 2024
								order by b.anoc, b.sigla, b.convit, a.idarquivo`)
	if err != nil {
		Error(err)
	}

	aux2, err := cnxFdb.Query(`select mascmod, numlic from cadlic where mascmod containing '/'`)
	if err != nil {
		Error(err)
	}
	numlics := make(map[string]int)
	for aux2.Next() {
		var mascmod string
		var numlic int
		err := aux2.Scan(&mascmod, &numlic)
		if err != nil {
			Error(err)
		}
		numlics[mascmod] = numlic
	}

	aux3, err := cnxFdb.Query(`select numlic, proclic from cadlic`)
	if err != nil {
		Error(err)
	}
	proclics := make(map[int]string)
	for aux3.Next() {
		var numlic int
		var proclic string
		err := aux3.Scan(&numlic, &proclic)
		if err != nil {
			Error(err)
		}
		proclics[numlic] = proclic
	}

	insert, err := cnxFdb.Prepare(`insert into documentos_rtf (numero, tipo, chave, datadoc, arqdigital, tparqdigital, numlic) values (?, ?, ?, ?, ?, 'PDF', ?)`)
	if err != nil {
		Error(err)
	}

	var numero int
	var descricao string
	var tipo int
	var datadoc string
	var arquivo []byte
	var mascmod string

	for rows.Next() {
		err := rows.Scan(&numero, &descricao, &datadoc, &arquivo, &mascmod)
		if err != nil {
			Error(err)
		}
		tipo = tipos[descricao]
		_, ok := insert.Exec(numero, tipo, proclics[numlics[mascmod]], datadoc, arquivo, numlics[mascmod])
		if ok != nil {
			Error(ok)
		}
	}
	cnxFdb.Close()
}

func TipoArquivos() {
	cnxFdb, err := ConexaoFirebird()
	if err != nil {
		Error(err)
	}

	cnxSql, err := ConexaoSqlServer()
	if err != nil {
		Error(err)
	}

	tipArquivos, err := cnxSql.Query(`select
											distinct b.descricao
										from
											mat.MXT04900 a
										join mat.MXT05000 b on
											a.idTipoArquivo = b.idTipoArquivo
										where a.idAgenda is not null and b.descricao <> 'Edital'`)
	if err != nil {
		Error(err)
	}
	defer tipArquivos.Close()

	insertTipoArquivo, err := cnxFdb.Prepare(`insert into MODELO_RTF (codigo, titulo, cabecalho, controle) values (?, ?, ?, ?)`)
	if err != nil {
		Error(err)
	}

	var codArquivo int 
	cnxFdb.QueryRow(`select max(codigo) from MODELO_RTF`).Scan(&codArquivo)
	var titulo string

	for tipArquivos.Next() {
		codArquivo ++
		err := tipArquivos.Scan(&titulo)
		if err != nil {
			Error(err)
		}

		_, ok := insertTipoArquivo.Exec(codArquivo, titulo, "0", "LICITACAO")
		if ok != nil {
			Error(ok)
		}
	}
	cnxFdb.Close()
}

func ConexaoFirebird() (*sql.DB, error) {
	host := "localhost:3050"
	user := "FSCSCPI8"
	password := "scpi"
	// dbName := `C:\Fiorilli\SCPI_8\Cidades\CM - BOTUCATU\ARQ2024\SCPI2024.FDB` // Minha Base
	// dbName := `C:\Fiorilli\SCPI_8\Cidades\BOTUCATU\ARQ2024\SCPI2024.FDB` // Ednilso
	dbName := `C:\Fiorilli\BANCOS\SCPI\DADOS\CAMARA\ARQ2023\SCPI2023.FDB` // Prefeitura

	connString := fmt.Sprintf("%s:%s@%s/%s", user, password, host, dbName)

	conFdb, err := sql.Open("firebirdsql", connString)
	if err != nil {
		return nil, err
	}

	err = conFdb.Ping()
	if err != nil {
		fmt.Println("Erro:", err)
		return nil, err
	}
	fmt.Println("CONEXÃO ESTABELECIDA")

	return conFdb, nil
}

func ConexaoSqlServer() (*sql.DB, error) {
	Cnx_dest, err := sql.Open("sqlserver", ("server=170.0.48.166;user=sa;password=@mendola#2022;port=1433;database=smar_comprasCM"))

	if err != nil {
		var input string
		fmt.Println(` `)
		fmt.Println(err)
		fmt.Println("Press 'Enter' to exit...")
		fmt.Scanln(&input)

	}

	err = Cnx_dest.Ping()
	if err != nil {
		var input string
		fmt.Println(` `)
		fmt.Println(err)
		fmt.Println("Press 'Enter' to exit...")
		fmt.Scanln(&input)

	}
	fmt.Printf("CONEXÃO ESTABELECIDA\n")

	return Cnx_dest, err
}

func Error(err error) {
	var input string
	fmt.Println(` `)
	fmt.Println(err)
	fmt.Println("Press 'Enter' to exit...")
	fmt.Scanln(&input)
}