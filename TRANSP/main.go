package main

import (
	"GO_TRANSP_ANEXOS/conexao"
	"fmt"
	"os"
	"time"
)

func main() {
	Cnx, err := conexao.Conexao()
	if err != nil {
		panic(err)
	}

	var id int
	Cnx.QueryRow(`SELECT MAX(ID_ATO) FROM TRANSP_ATOS_PUBLICACOES`).Scan(&id)

	anexos, err := os.ReadDir("C:\\Decretos")
	if err != nil {	
		panic(err)
	}

	tx, _ := Cnx.Begin()
	for _, anexo := range anexos {
		var arquivo []byte
		var data_pub time.Time
		var descricao string
		
		id++
		arquivo, err = os.ReadFile(fmt.Sprintf("C:\\Decretos\\%s",anexo.Name()))
		if err != nil {
			panic(err)
		}

		info, err := anexo.Info()
        if err != nil {
            panic(err)
        }

		descricao = info.Name()
		data_pub = info.ModTime()

		_, err = Cnx.Exec(`INSERT INTO TRANSP_ATOS_PUBLICACOES (ID_ATO, TIPO, DESCRICAO, DATA_PUBLICACAO, ARQUIVO, ARQUIVO_TIPO, EMPRESA) VALUES (?, ?, ?, ?, ?, ?, ?)`, id, "DECRETO", descricao, data_pub, arquivo, "PDF", 2)
		if err != nil {
			tx.Rollback()
			panic(err)
		}
	}
	tx.Commit()
	fmt.Println("Anexos inseridos com sucesso!")
	defer Cnx.Close()
}