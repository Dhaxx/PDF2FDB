package main

import (
	"PDF2FDB/conexao"
	"fmt"
)

func main() {
	converterAnexos()
}

func converterAnexos() {
	anexos, err := conexao.CnxSql.Query((`select case when tippat = 'I' then 3 else 1 end tippat, rtrim(Codigo) codigo, Imagem, Desc_imagem from mat.MPT80700`)) // BUSCA ANEXOS
	if err != nil {
		panic(err)
	}
	
	//defer anexos.Close()

	type Key struct {
		Int int
		Str string
	}
	chapas := make(map[Key]string) // MAPA DE CHAPAS

	// BUSCA CHAPAS DOS MÓVEIS E IMÓVEIS
	aux1, err := conexao.CnxPatr.Query(`select chapa_pat_alt, codigo_gru_pat, chapa_pat from pt_cadpat where codigo_gru_pat in (1,2,3) and chapa_pat_alt is not null`) 
	if err != nil {
		panic(err)
	}
	//defer aux1.Close()

	// PREENCHE MAPA DE CHAPAS
	for aux1.Next() {
		var chapa_pat, chapa_pat_alt string
		var codigo_gru_pat int
		err := aux1.Scan(&chapa_pat_alt, &codigo_gru_pat, &chapa_pat)
		if err != nil {
			panic(err)
		}
		chapas[Key{codigo_gru_pat, chapa_pat_alt}] = chapa_pat
	}

	maxCodigoArq := conexao.CnxAnx.QueryRow(`select COALESCE(max(CODIGO_ARQ),0)+1 from patr_arquivos`)
	var codigo_arq int
	err = maxCodigoArq.Scan(&codigo_arq)
	if err != nil {
		panic(err)
	}

	insert, err := conexao.CnxAnx.Prepare(`insert into patr_arquivos (codigo_arq, natur, chapa, descricao, arquivo, empresa, tipo_arq) values (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		panic(err)
	}
	
	//defer insert.Close()

	var tippat int
	var chapa_alt, descricao string
	var arquivo []byte

	// INÍCIO DO LOOP DE INSERÇÃO DOS ANEXOS
	tx, _ := conexao.CnxAnx.Begin()
	for anexos.Next() {
		err = anexos.Scan(&tippat, &chapa_alt, &arquivo, &descricao)
		if err != nil {
			panic(err)
		}

		key := Key{Int: tippat, Str: chapa_alt}
		chapa, ok := chapas[key]
		if !ok {
			tippat = 2
			key = Key{Int: tippat, Str: chapa_alt}
			chapa, ok = chapas[key]
			if !ok {
				continue
			}
		}
		tippatStr := fmt.Sprintf("%03d", tippat)

		_, err = insert.Exec(codigo_arq, tippatStr, chapa, descricao, arquivo, 2, "PDF")
		if err != nil {
			panic(err)
		}

		codigo_arq++

		fmt.Println(codigo_arq)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	defer anexos.Close()
	conexao.CnxAnx.Close()
}