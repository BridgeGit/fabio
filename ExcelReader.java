package it.terna.gstat.excel;

import static it.terna.gstat.excel.Markers.EN_CD_CALORE_DIGESTORE;
import static it.terna.gstat.excel.Markers.EN_CE_CODICE_ATECUE;
import static it.terna.gstat.excel.Markers.EN_CE_KW_CONSEGNATI;
import static it.terna.gstat.excel.Markers.EN_CE_NUMERO_UTENZE;
import static it.terna.gstat.excel.Markers.EN_CE_PERIODO;
import static it.terna.gstat.excel.Markers.EN_CE_PROVINCIA;
import static it.terna.gstat.excel.Markers.EN_CE_TITOLO;
import static it.terna.gstat.excel.Markers.EN_COMMON_CENSIMP_IMPIANTO;
import static it.terna.gstat.excel.Markers.EN_COMMON_MESE;
import static it.terna.gstat.excel.Markers.EN_COMMON_NOME_IMPIANTO;
import static it.terna.gstat.excel.Markers.EN_COMMON_SEZIONE;
import static it.terna.gstat.excel.Markers.EN_COMMON_SEZIONE_VECCHIO_GSTAT;
import static it.terna.gstat.excel.Markers.EN_COMMON_SOTTOTIPO_SEZIONE;
import static it.terna.gstat.excel.Markers.EN_COMMON_TIPO_IMPIANTO;
import static it.terna.gstat.excel.Markers.EN_CU_COMBUSTIBILE_UTILIZZATO;
import static it.terna.gstat.excel.Markers.EN_CU_CSE;
import static it.terna.gstat.excel.Markers.EN_CU_PCI;
import static it.terna.gstat.excel.Markers.EN_CU_QUANTITA_IMPIEGATA;
import static it.terna.gstat.excel.Markers.EN_CU_RENDIMENTO_CALDAIA;
import static it.terna.gstat.excel.Markers.EN_CU_UNITA_MISURA_COMB;
import static it.terna.gstat.excel.Markers.EN_CU_UNITA_MISURA_PCI;
import static it.terna.gstat.excel.Markers.EN_PO_CONSUMO_POMPAGGIO;
import static it.terna.gstat.excel.Markers.EN_PR_DI_CUI_ASSORBITI;
import static it.terna.gstat.excel.Markers.EN_PR_ENERGIA_ASSORBITA_PER_SA;
import static it.terna.gstat.excel.Markers.EN_PR_ENERGIA_PRELEVATA_DALLA_RETE;
import static it.terna.gstat.excel.Markers.EN_PR_EROGAZIONE_SU_RETE_PUBBLICA;
import static it.terna.gstat.excel.Markers.EN_PR_IMMESSA_SU_RETE_PUBBLICA;
import static it.terna.gstat.excel.Markers.EN_PR_POTENZA_EFFICIENTE_LORDA;
import static it.terna.gstat.excel.Markers.EN_PR_PRODUZIONE_LORDA;
import static it.terna.gstat.excel.Markers.EN_UC_CODICE_ATECUE;
import static it.terna.gstat.excel.Markers.EN_UC_NUMERO_UTENZE;
import static it.terna.gstat.excel.Markers.EN_UC_PROVINCIA;
import static it.terna.gstat.excel.Markers.EN_UC_QUANTITA;
import static it.terna.gstat.excel.Markers.EN_UC_TIPO_UTILIZZO;
import static it.terna.gstat.excel.Markers.EN_UC_USO_FINALE;
import static it.terna.gstat.excel.Markers.EN_UE_CODICE_ATECUE;
import static it.terna.gstat.excel.Markers.EN_UE_NUMERO_UTENZE;
import static it.terna.gstat.excel.Markers.EN_UE_PROVINCIA;
import static it.terna.gstat.excel.Markers.EN_UE_QUANTITA;
import static it.terna.gstat.excel.Markers.EN_UE_TIPOLOGIA_AUTOCONSUMO;
import it.terna.gstat.entities.SysBatchDatiFlusso;
import it.terna.gstat.entities.XlsCaloreDigestore;
import it.terna.gstat.entities.XlsCombustUtilizzati;
import it.terna.gstat.entities.XlsConsegnaEnergia;
import it.terna.gstat.entities.XlsEnergiaProdotta;
import it.terna.gstat.entities.XlsExportTemplate;
import it.terna.gstat.entities.XlsPompaggi;
import it.terna.gstat.entities.XlsUtilizziCalore;
import it.terna.gstat.entities.XlsUtilizzoEnergia;
import it.terna.gstat.exceptions.InvalidExcelException;
import it.terna.gstat.presentation.beans.statisticheAnnuali.InvioDatiExcelProduttoreBean;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.primefaces.model.UploadedFile;

import com.monitorjbl.xlsx.StreamingReader;

public class ExcelReader {

	public static ArrayList<XlsEnergiaProdotta> parsaProdTEnergiaProdotta(UploadedFile file, SysBatchDatiFlusso df, XlsExportTemplate template) throws IOException, InvalidExcelException {
		try {
			Workbook wb = null;
			try {
				wb = new HSSFWorkbook(file.getInputstream());
			}
			catch (Exception e){
				wb = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(file.getInputstream());
			}

			//prendo tab energia prodotta

			Sheet sheet = null;
			try {
				sheet = wb.getSheetAt(2);
				if (sheet==null){
					throw new InvalidExcelException("Sheet Energia Prodotta non presente");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Sheet Energia Prodotta non presente");
			}

			Iterator<Row> rowsIter = sheet.rowIterator();

			//salto le prime due righe di intestazione
			skip (rowsIter, 2);
			ArrayList<XlsEnergiaProdotta> data = new ArrayList<XlsEnergiaProdotta>();

			int indice = 0;
			while (rowsIter.hasNext()){
				indice ++;
				Row myRow = rowsIter.next();
				if (rowIsEmpty(myRow)){
					continue;
				}
				if(!verifyIfRowProdEnergiaProdottaIsEmpty(myRow, indice, template))
				{
					XlsEnergiaProdotta e = new XlsEnergiaProdotta();
					e.setSysBatchDatiFlusso(df);

					e.setCodiceCensimpImpianto(getStringValue (myRow, EN_COMMON_CENSIMP_IMPIANTO));
					e.setNomeImpianto(getStringValue (myRow, EN_COMMON_NOME_IMPIANTO));
					e.setSezione(getStringValue (myRow, EN_COMMON_SEZIONE));
					e.setSezioneVecchioGstat(getStringValue (myRow, EN_COMMON_SEZIONE_VECCHIO_GSTAT));
					e.setTipoImpianto(getStringValue (myRow, EN_COMMON_TIPO_IMPIANTO));
					e.setSottotipoSezione(getStringValue (myRow, EN_COMMON_SOTTOTIPO_SEZIONE));
					e.setMese(getStringValue (myRow, EN_COMMON_MESE));

					e.setErogazioneSuRetePubblica(getStringValue(myRow, EN_PR_EROGAZIONE_SU_RETE_PUBBLICA));
					e.setPotenzaEfficienteLordaKw(getBigDecimalValue(myRow, EN_PR_POTENZA_EFFICIENTE_LORDA));
					e.setProduzioneLordaKwh(getBigDecimalValue(myRow, EN_PR_PRODUZIONE_LORDA));
					e.setEnergiaAssorbitaPerSa(getBigDecimalValue(myRow, EN_PR_ENERGIA_ASSORBITA_PER_SA));
					e.setDiCuiAssorbiti(getBigDecimalValue(myRow, EN_PR_DI_CUI_ASSORBITI));
					e.setEnergiaPrelevataDallaRete(getBigDecimalValue(myRow, EN_PR_ENERGIA_PRELEVATA_DALLA_RETE));
					e.setImmessaSuRetePubblica(getBigDecimalValue(myRow, EN_PR_IMMESSA_SU_RETE_PUBBLICA));

					data.add(e);
				}
			}

			if (template==null) return data;
			try {
				if(data.size()+2 != template.getNumRigheSht1()){
					throw new InvalidExcelException("Nello Sheet Energia Prodotta il numero di righe esportate è diverso da quello importate");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Nello Sheet Energia Prodotta il numero di righe esportate è diverso da quello importate");
			}

			return data;
		}
		finally {
			file.getInputstream().close();
		}
	}

	public static ArrayList<XlsUtilizzoEnergia> parsaProdTUtilizzoEnergia(UploadedFile file, SysBatchDatiFlusso df, XlsExportTemplate template) throws IOException, InvalidExcelException {
		try {
			Workbook wb = null;
			try {
				wb = new HSSFWorkbook(file.getInputstream());
			}
			catch (Exception e){
				wb = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(file.getInputstream());
			}

			//prendo tab energia prodotta
			Sheet sheet = null;
			try {
				sheet = wb.getSheetAt(3);
				if (sheet==null){
					throw new InvalidExcelException("Sheet Utilizzo Energia non presente");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Sheet Utilizzo Energia non presente");
			}

			Iterator<Row> rowsIter = sheet.rowIterator();

			//salto le prime due righe di intestazione
			skip (rowsIter, 2);
			ArrayList<XlsUtilizzoEnergia> data = new ArrayList<XlsUtilizzoEnergia>();

			int indice=0;
			while (rowsIter.hasNext()){
				indice++;
				Row myRow = rowsIter.next();
				if (rowIsEmpty(myRow)){
					continue;
				}
				if(!verifyIfRowProdUtilizzoEnergiaIsEmpty(myRow, indice, template)){
					XlsUtilizzoEnergia e = new XlsUtilizzoEnergia(); 
					e.setSysBatchDatiFlusso(df);

					e.setCodiceCensimpImpianto(getStringValue (myRow, EN_COMMON_CENSIMP_IMPIANTO));
					e.setNomeImpianto(getStringValue (myRow, EN_COMMON_NOME_IMPIANTO));
					e.setSezione(getStringValue (myRow, EN_COMMON_SEZIONE));
					e.setSezioneVecchioGstat(getStringValue (myRow, EN_COMMON_SEZIONE_VECCHIO_GSTAT));
					e.setTipoImpianto(getStringValue (myRow, EN_COMMON_TIPO_IMPIANTO));
					e.setSottotipoSezione(getStringValue (myRow, EN_COMMON_SOTTOTIPO_SEZIONE));
					e.setMese(getStringValue (myRow, EN_COMMON_MESE));

					e.setTipologiaAutoconsumo(getStringValue(myRow, EN_UE_TIPOLOGIA_AUTOCONSUMO));
					e.setProvincia(getStringValue(myRow, EN_UE_PROVINCIA));
					e.setCodiceAtecue(getStringValue(myRow, EN_UE_CODICE_ATECUE));
					e.setNumeroUtenze(getBigDecimalValue(myRow, EN_UE_NUMERO_UTENZE));
					e.setQuantitaKwh(getBigDecimalValue(myRow, EN_UE_QUANTITA));

					data.add(e);
				}
			}

			if (template==null) return data;
			try {
				if(data.size()+2 != template.getNumRigheSht2()){
					throw new InvalidExcelException("Nello Sheet Utilizzo dell' Energia il numero di righe esportate è diverso da quello importate");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Nello Sheet Utilizzo dell' Energia il numero di righe esportate è diverso da quello importate");
			}

			return data;
		}
		finally {
			file.getInputstream().close();
		}
	}

	public static ArrayList<XlsCombustUtilizzati> parsaProdTCombustibiliUtilizzati(UploadedFile file, SysBatchDatiFlusso df, XlsExportTemplate template) throws IOException, InvalidExcelException {
		try {
			Workbook wb = null;
			try {
				wb = new HSSFWorkbook(file.getInputstream());
			}
			catch (Exception e){
				wb = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(file.getInputstream());
			}

			//prendo tab energia prodotta
			Sheet sheet = null;
			try {
				sheet = wb.getSheetAt(4);
				if (sheet==null){
					throw new InvalidExcelException("Sheet Combustibili Utilizzati non presente");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Sheet Combustibili Utilizzati non presente");
			}

			Iterator<Row> rowsIter = sheet.rowIterator();

			//salto le prime due righe di intestazione
			skip (rowsIter, 1);
			ArrayList<XlsCombustUtilizzati> data = new ArrayList<XlsCombustUtilizzati>();
			int indice=0;
			while (rowsIter.hasNext()){
				indice++;
				Row myRow = rowsIter.next();
				if (rowIsEmpty(myRow)){
					continue;
				}
				if(!verifyIfRowProdCombustibiliUtilizzatiIsEmpty(myRow, indice, template)){
					XlsCombustUtilizzati e = new XlsCombustUtilizzati(); 
					e.setSysBatchDatiFlusso(df);

					e.setCodiceCensimpImpianto(getStringValue (myRow, EN_COMMON_CENSIMP_IMPIANTO));
					e.setNomeImpianto(getStringValue (myRow, EN_COMMON_NOME_IMPIANTO));
					e.setSezione(getStringValue (myRow, EN_COMMON_SEZIONE));
					e.setSezioneVecchioGstat(getStringValue (myRow, EN_COMMON_SEZIONE_VECCHIO_GSTAT));
					e.setTipoImpianto(getStringValue (myRow, EN_COMMON_TIPO_IMPIANTO));
					e.setSottotipoSezione(getStringValue (myRow, EN_COMMON_SOTTOTIPO_SEZIONE));
					e.setMese(getStringValue (myRow, EN_COMMON_MESE));

					e.setCombustibileUtilizzato(getStringValue(myRow, EN_CU_COMBUSTIBILE_UTILIZZATO));
					e.setUnitaDiMisuraPci(getStringValue(myRow, EN_CU_UNITA_MISURA_PCI));
					e.setPotereCalorificoInferiore(getBigDecimalValue(myRow, EN_CU_PCI));
					e.setConsumoSpecificoElettrico(getBigDecimalValue(myRow, EN_CU_CSE));
					e.setRendimentoDiCaldaia(getBigDecimalValue(myRow, EN_CU_RENDIMENTO_CALDAIA));
					e.setUnitaDiMisuraCombustibile(getStringValue(myRow, EN_CU_UNITA_MISURA_COMB));
					e.setQuantitaImpiegata(getBigDecimalValue(myRow, EN_CU_QUANTITA_IMPIEGATA));

					data.add(e);
				}
			}

			if (template==null) return data;
			try {
				if(data.size()+1 != template.getNumRigheSht3()){
					throw new InvalidExcelException("Nello Sheet Combustibili Utilizzati il numero di righe esportate è diverso da quello importate");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Nello Sheet Combustibili Utilizzati il numero di righe esportate è diverso da quello importate");
			}

			return data;
		}
		finally {
			file.getInputstream().close();
		}
	}

	public static ArrayList<XlsUtilizziCalore> parsaProdTUtilizziCalore(UploadedFile file, SysBatchDatiFlusso df, XlsExportTemplate template) throws IOException, InvalidExcelException {
		try {
			Workbook wb = null;
			try {
				wb = new HSSFWorkbook(file.getInputstream());
			}
			catch (Exception e){
				wb = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(file.getInputstream());
			}

			//prendo tab energia prodotta
			Sheet sheet = null;
			try {
				sheet = wb.getSheetAt(5);
				if (sheet==null){
					throw new InvalidExcelException("Sheet utilizzo calore non presente");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Sheet utilizzo calore non presente");
			}

			Iterator<Row> rowsIter = sheet.rowIterator();

			//salto le prime due righe di intestazione
			skip (rowsIter, 2);
			ArrayList<XlsUtilizziCalore> data = new ArrayList<XlsUtilizziCalore>();

			int indice=0;
			while (rowsIter.hasNext()){
				indice++;
				Row myRow = rowsIter.next();
				if (rowIsEmpty(myRow)){
					continue;
				}
				if (rowMessaggio(myRow)){
					continue;
				}
				if(!verifyIfRowProdTUtilizziCaloreIsEmpty(myRow, indice, template)){
					XlsUtilizziCalore e = new XlsUtilizziCalore(); 
					e.setSysBatchDatiFlusso(df);

					e.setCodiceCensimpImpianto(getStringValue (myRow, EN_COMMON_CENSIMP_IMPIANTO));
					e.setNomeImpianto(getStringValue (myRow, EN_COMMON_NOME_IMPIANTO));
					e.setSezione(getStringValue (myRow, EN_COMMON_SEZIONE));
					e.setSezioneVecchioGstat(getStringValue (myRow, EN_COMMON_SEZIONE_VECCHIO_GSTAT));
					e.setTipoImpianto(getStringValue (myRow, EN_COMMON_TIPO_IMPIANTO));
					e.setSottotipoSezione(getStringValue (myRow, EN_COMMON_SOTTOTIPO_SEZIONE));
					e.setMese(getStringValue (myRow, EN_COMMON_MESE));

					e.setTipoUtilizzo(getStringValue(myRow, EN_UC_TIPO_UTILIZZO));
					e.setUsoFinale(getStringValue(myRow, EN_UC_USO_FINALE));
					e.setProvincia(getStringValue(myRow, EN_UC_PROVINCIA));
					e.setCodiceAtecue(getStringValue(myRow, EN_UC_CODICE_ATECUE));
					e.setNumeroUtenze(getBigDecimalValue(myRow, EN_UC_NUMERO_UTENZE));
					e.setQuantita(getBigDecimalValue(myRow, EN_UC_QUANTITA));

					data.add(e);
				}
			}

			if (template==null) return data;
			try {
				if(data.size()+2 != template.getNumRigheSht4()){
					throw new InvalidExcelException("Nello Sheet utilizzo del calore il numero di righe esportate è diverso da quello importate");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Nello Sheet utilizzo del calore il numero di righe esportate è diverso da quello importate");
			}

			return data;
		}
		finally {
			file.getInputstream().close();
		}
	}

	public static ArrayList<XlsCaloreDigestore> parsaProdTCaloreDigestore(UploadedFile file, SysBatchDatiFlusso df, XlsExportTemplate template) throws IOException, InvalidExcelException {
		try {
			Workbook wb = null;
			try {
				wb = new HSSFWorkbook(file.getInputstream());
			}
			catch (Exception e){
				wb = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(file.getInputstream());
			}

			//prendo tab energia prodotta
			Sheet sheet = null;
			try {
				sheet = wb.getSheetAt(6);
				if (sheet==null){
					throw new InvalidExcelException("Sheet calore digestore non presente");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Sheet calore digestore non presente");
			}

			Iterator<Row> rowsIter = sheet.rowIterator();

			//salto le prime due righe di intestazione
			skip (rowsIter, 2);
			ArrayList<XlsCaloreDigestore> data = new ArrayList<XlsCaloreDigestore>();

			int indice = 0;
			while (rowsIter.hasNext()){
				indice++;
				Row myRow = rowsIter.next();
				if (rowIsEmpty(myRow)){
					continue;
				}
				if(!verifyIfRowProdTCaloreDigestoreIsEmpty(myRow, indice, template)){
					XlsCaloreDigestore e = new XlsCaloreDigestore(); 
					e.setSysBatchDatiFlusso(df);

					e.setCodiceCensimpImpianto(getStringValue (myRow, EN_COMMON_CENSIMP_IMPIANTO));
					e.setNomeImpianto(getStringValue (myRow, EN_COMMON_NOME_IMPIANTO));
					e.setSezione(getStringValue (myRow, EN_COMMON_SEZIONE));
					e.setSezioneVecchioGstat(getStringValue (myRow, EN_COMMON_SEZIONE_VECCHIO_GSTAT));
					e.setTipoImpianto(getStringValue (myRow, EN_COMMON_TIPO_IMPIANTO));
					e.setSottotipoSezione(getStringValue (myRow, EN_COMMON_SOTTOTIPO_SEZIONE));
					e.setMese(getStringValue (myRow, EN_COMMON_MESE));

					e.setCaloreDigestore(getBigDecimalValue(myRow, EN_CD_CALORE_DIGESTORE));

					data.add(e);
				}
			}

			if (template==null) return data;
			try {
				if(data.size()+2 != template.getNumRigheSht5()){
					throw new InvalidExcelException("Nello Sheet calore digestore il numero di righe esportate è diverso da quello importate");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Nello Sheet calore digestore il numero di righe esportate è diverso da quello importate");
			}

			return data;
		}
		finally {
			file.getInputstream().close();
		}
	}

	public static ArrayList<XlsPompaggi> parsaProdXPompaggi(UploadedFile file, SysBatchDatiFlusso df, XlsExportTemplate template) throws IOException, InvalidExcelException {
		try {
			Workbook wb = null;
			try {
				wb = new HSSFWorkbook(file.getInputstream());
			}
			catch (Exception e){
				wb = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(file.getInputstream());
			}

			//prendo tab energia prodotta
			Sheet sheet = null;
			try {
				sheet = wb.getSheetAt(4);
				if (sheet==null){
					throw new InvalidExcelException("Sheet pompaggi non presente");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Sheet pompaggi non presente");
			}

			Iterator<Row> rowsIter = sheet.rowIterator();

			//salto le prime due righe di intestazione
			skip (rowsIter, 1);
			ArrayList<XlsPompaggi> data = new ArrayList<XlsPompaggi>();

			int indice=0;
			while (rowsIter.hasNext()){
				indice++;
				Row myRow = rowsIter.next();
				if (rowIsEmpty(myRow)){
					continue;
				}
				if(!verifyIfRowProdXPompaggiIsEmpty(myRow, indice, template)){
					XlsPompaggi e = new XlsPompaggi(); 
					e.setSysBatchDatiFlusso(df);

					e.setCodiceCensimpImpianto(getStringValue (myRow, EN_COMMON_CENSIMP_IMPIANTO));
					e.setNomeImpianto(getStringValue (myRow, EN_COMMON_NOME_IMPIANTO));
					e.setSezione(getStringValue (myRow, EN_COMMON_SEZIONE));
					e.setSezioneVecchioGstat(getStringValue (myRow, EN_COMMON_SEZIONE_VECCHIO_GSTAT));
					e.setTipoImpianto(getStringValue (myRow, EN_COMMON_TIPO_IMPIANTO));
					e.setSottotipoSezione(getStringValue (myRow, EN_COMMON_SOTTOTIPO_SEZIONE));
					e.setMese(getStringValue (myRow, EN_COMMON_MESE));

					e.setConsumoPerPompaggio(getBigDecimalValue(myRow, EN_PO_CONSUMO_POMPAGGIO));

					data.add(e);
				}
			}

			if (template==null) return data;
			try {
				if(data.size()+1 != template.getNumRigheSht3()){
					throw new InvalidExcelException("Nello Sheet pompaggi il numero di righe esportate è diverso da quello importate");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Nello Sheet pompaggi il numero di righe esportate è diverso da quello importate");
			}

			return data;
		}
		finally {
			file.getInputstream().close();
		}
	}

	public static ArrayList<XlsConsegnaEnergia> parsaDistrConsegnaEnergia(UploadedFile file, SysBatchDatiFlusso df, XlsExportTemplate template) throws IOException, InvalidExcelException {
		try {
			Workbook wb = null;
			try {
				wb = new HSSFWorkbook(file.getInputstream());
			}
			catch (Exception e){
				wb = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(file.getInputstream());
			}

			//prendo tab energia prodotta
			Sheet sheet = null;
			try {
				sheet = wb.getSheetAt(2);
				if (sheet==null){
					throw new InvalidExcelException("Sheet consegna energia non presente");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Sheet consegna energia non presente");
			}

			Iterator<Row> rowsIter = sheet.rowIterator();

			//salto le prime due righe di intestazione
			skip (rowsIter, 2);
			ArrayList<XlsConsegnaEnergia> data = new ArrayList<XlsConsegnaEnergia>();
			int indice = 0;
			while (rowsIter.hasNext()){
				indice++;
				Row myRow = rowsIter.next();
				if (rowIsEmpty(myRow)){
					continue;
				}
				if(!verifyIfRowDistrConsegnaEnergiaIsEmpty(myRow, indice, template)){
					XlsConsegnaEnergia e = new XlsConsegnaEnergia(); 
					e.setSysBatchDatiFlusso(df);

					e.setPeriodo(getStringValue(myRow, EN_CE_PERIODO));
					e.setTitolo(getStringValue(myRow, EN_CE_TITOLO));
					e.setProvincia(getStringValue(myRow, EN_CE_PROVINCIA));
					e.setCodiceAtecue(getStringValue(myRow, EN_CE_CODICE_ATECUE));
					e.setNumeroUtenze(getBigDecimalValue(myRow, EN_CE_NUMERO_UTENZE));
					e.setKwhConsegnati(getBigDecimalValue(myRow, EN_CE_KW_CONSEGNATI));

					data.add(e);
				}
			}

			if (template==null) return data;
			try {
				if(data.size()+2 != template.getNumRigheSht1()){
					throw new InvalidExcelException("Nello Sheet consegna dell'Energia il numero di righe esportate è diverso da quello importate");
				}
			}
			catch (IllegalArgumentException e){
				throw new InvalidExcelException("Nello Sheet consegna dell'Energia il numero di righe esportate è diverso da quello importate");
			}

			return data;
		}
		finally {
			file.getInputstream().close();
		}
	}

	public static boolean rowIsEmpty(Row myRow) {
		int lastCellNum = getLastCellNum(myRow);
		for (int i=0; i<=lastCellNum; i++){
			Cell cell = myRow.getCell(i);
			if (cell!=null && !cell.toString().trim().equals("")){
				return false;
			}
		}
		return true;
	}

	private static boolean rowMessaggio(Row myRow) {
		int lastCellNum = getLastCellNum(myRow);
		if (lastCellNum>=0){
			Cell cell = myRow.getCell(0);
			if (cell!=null){
				if (cell.toString().equals(InvioDatiExcelProduttoreBean.MESS_RIGA_1) ||
						cell.toString().equals(InvioDatiExcelProduttoreBean.MESS_RIGA_2) ||
						cell.toString().equals(InvioDatiExcelProduttoreBean.MESS_RIGA_3) ||
						cell.toString().equals(InvioDatiExcelProduttoreBean.MESS_RIGA_4)){
					return true;
				}
			}
		}
		return false;
	}

	public static boolean verifyIfRowProdEnergiaProdottaIsEmpty(Row myRow, int indice, XlsExportTemplate template) throws InvalidExcelException{
		boolean result = false;

		if(getLastCellNum(myRow) > Markers.EN_PR_COLTOT){
			throw new InvalidExcelException("Il numero di colonne di una o più righe esportate è diverso da quello importate");
		}

		if (template==null) return result;

		if(template.getNumRigheSht1() >= indice + 2){
			if(myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO)==null || myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_NOME_IMPIANTO)==null || myRow.getCell(EN_COMMON_NOME_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE)==null || myRow.getCell(EN_COMMON_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT)==null || myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_TIPO_IMPIANTO)==null || myRow.getCell(EN_COMMON_TIPO_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE)==null || myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_MESE)==null || myRow.getCell(EN_COMMON_MESE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_PR_EROGAZIONE_SU_RETE_PUBBLICA)==null || myRow.getCell(EN_PR_EROGAZIONE_SU_RETE_PUBBLICA).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_PR_POTENZA_EFFICIENTE_LORDA)==null || myRow.getCell(EN_PR_POTENZA_EFFICIENTE_LORDA).toString().trim().equals(""))
				result = true;
		}
		else
		{
			throw new InvalidExcelException("Nello Sheet Energia Prodotta il numero di righe esportate è diverso da quello importate");
		}
		return result;
	}

	public static boolean verifyIfRowProdUtilizzoEnergiaIsEmpty(Row myRow, int indice, XlsExportTemplate template) throws InvalidExcelException{
		boolean result = false;

		if(getLastCellNum(myRow) > Markers.EN_UE_COLTOT){
			throw new InvalidExcelException("Il numero di colonne di una o più righe esportate è diverso da quello importate");
		}
		if (template==null) return result;

		if(template.getNumRigheSht2() >= indice+ 2){
			if(myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO)==null || myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_NOME_IMPIANTO)==null || myRow.getCell(EN_COMMON_NOME_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE)==null || myRow.getCell(EN_COMMON_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT)==null || myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_TIPO_IMPIANTO)==null || myRow.getCell(EN_COMMON_TIPO_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE)==null || myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_MESE)==null || myRow.getCell(EN_COMMON_MESE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_UE_TIPOLOGIA_AUTOCONSUMO)==null || myRow.getCell(EN_UE_TIPOLOGIA_AUTOCONSUMO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_UE_PROVINCIA)==null || myRow.getCell(EN_UE_PROVINCIA).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_UE_CODICE_ATECUE)==null || myRow.getCell(EN_UE_CODICE_ATECUE).toString().trim().equals(""))
				result = true;
		}
		else
		{
			throw new InvalidExcelException("Nello Sheet Utilizzo dell' Energia il numero di righe esportate è diverso da quello importate");
		}
		return result;
	}

	public static boolean verifyIfRowProdCombustibiliUtilizzatiIsEmpty(Row myRow, int indice, XlsExportTemplate template) throws InvalidExcelException{
		boolean result = false;

		if(getLastCellNum(myRow) > Markers.EN_CU_COLTOT){
			throw new InvalidExcelException("Il numero di colonne di una o più righe esportate è diverso da quello importate");
		}
		if (template==null) return result;

		if(template.getNumRigheSht3() >= indice + 1){
			if(myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO)==null || myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_NOME_IMPIANTO)==null || myRow.getCell(EN_COMMON_NOME_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE)==null || myRow.getCell(EN_COMMON_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_TIPO_IMPIANTO)==null || myRow.getCell(EN_COMMON_TIPO_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT)==null || myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE)==null || myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_MESE)==null || myRow.getCell(EN_COMMON_MESE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_CU_COMBUSTIBILE_UTILIZZATO)==null || myRow.getCell(EN_CU_COMBUSTIBILE_UTILIZZATO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_CU_UNITA_MISURA_PCI)==null || myRow.getCell(EN_CU_UNITA_MISURA_PCI).toString().trim().equals(""))
				result = true;
		}
		else{
			throw new InvalidExcelException("Nello Sheet Combustibili Utilizzati il numero di righe esportate è diverso da quello importate");
		}

		return result;
	}

	public static boolean verifyIfRowProdTUtilizziCaloreIsEmpty(Row myRow, int indice, XlsExportTemplate template) throws InvalidExcelException{
		boolean result = false;

		if(getLastCellNum(myRow) > Markers.EN_UC_COLTOT){
			throw new InvalidExcelException("Il numero di colonne di una o più righe esportate è diverso da quello importate");
		}
		if (template==null) return result;

		if(template.getNumRigheSht4() >= indice + 2){
			if(myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO)==null || myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_NOME_IMPIANTO)==null || myRow.getCell(EN_COMMON_NOME_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE)==null || myRow.getCell(EN_COMMON_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_TIPO_IMPIANTO)==null || myRow.getCell(EN_COMMON_TIPO_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT)==null || myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE)==null || myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_MESE)==null || myRow.getCell(EN_COMMON_MESE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_UC_TIPO_UTILIZZO)==null || myRow.getCell(EN_UC_TIPO_UTILIZZO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_UC_USO_FINALE)==null || myRow.getCell(EN_UC_USO_FINALE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_UC_PROVINCIA)==null || myRow.getCell(EN_UC_PROVINCIA).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_UC_CODICE_ATECUE)==null || myRow.getCell(EN_UC_CODICE_ATECUE).toString().trim().equals(""))
				result = true;
		}
		else
		{
			throw new InvalidExcelException("Nello Sheet utilizzo del calore il numero di righe esportate è diverso da quello importate");
		}
		return result;
	}

	public static boolean verifyIfRowProdTCaloreDigestoreIsEmpty(Row myRow, int indice, XlsExportTemplate template) throws InvalidExcelException{
		boolean result = false;

		if(getLastCellNum(myRow) > Markers.EN_CD_COLTOT){
			throw new InvalidExcelException("Il numero di colonne di una o più righe esportate è diverso da quello importate");
		}
		if (template==null) return result;

		if(template.getNumRigheSht5() >= indice + 2){
			if(myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO)==null || myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_NOME_IMPIANTO)==null || myRow.getCell(EN_COMMON_NOME_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE)==null || myRow.getCell(EN_COMMON_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_TIPO_IMPIANTO)==null || myRow.getCell(EN_COMMON_TIPO_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT)==null || myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE)==null || myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_MESE)==null || myRow.getCell(EN_COMMON_MESE).toString().trim().equals(""))
				result = true;
		}
		else
		{
			throw new InvalidExcelException("Nello Sheet calore digestore il numero di righe esportate è diverso da quello importate");
		}
		return result;
	}

	public static boolean verifyIfRowProdXPompaggiIsEmpty(Row myRow, int indice, XlsExportTemplate template) throws InvalidExcelException{
		boolean result = false;

		if(getLastCellNum(myRow) > Markers.EN_PO_COLTOT){
			throw new InvalidExcelException("Il numero di colonne di una o più righe esportate è diverso da quello importate");
		}
		if (template==null) return result;

		if(template.getNumRigheSht3() >= indice + 1){
			if(myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO)==null || myRow.getCell(EN_COMMON_CENSIMP_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_NOME_IMPIANTO)==null || myRow.getCell(EN_COMMON_NOME_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE)==null || myRow.getCell(EN_COMMON_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_TIPO_IMPIANTO)==null || myRow.getCell(EN_COMMON_TIPO_IMPIANTO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT)==null || myRow.getCell(EN_COMMON_SEZIONE_VECCHIO_GSTAT).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE)==null || myRow.getCell(EN_COMMON_SOTTOTIPO_SEZIONE).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_COMMON_MESE)==null || myRow.getCell(EN_COMMON_MESE).toString().trim().equals(""))
				result = true;
		}
		else
		{
			throw new InvalidExcelException("Nello Sheet pompaggi il numero di righe esportate è diverso da quello importate");
		}
		return result;
	}

	public static boolean verifyIfRowDistrConsegnaEnergiaIsEmpty(Row myRow, int indice, XlsExportTemplate template) throws InvalidExcelException{
		boolean result = false;

		if(getLastCellNum(myRow) > Markers.EN_CE_COLTOT){
			throw new InvalidExcelException("Il numero di colonne di una o più righe esportate è diverso da quello importate");
		}
		if (template==null) return result;

		if(template.getNumRigheSht1() >= indice + 2){
			if(myRow.getCell(EN_CE_PERIODO)==null || myRow.getCell(EN_CE_PERIODO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_CE_TITOLO)==null || myRow.getCell(EN_CE_TITOLO).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_CE_PROVINCIA)==null || myRow.getCell(EN_CE_PROVINCIA).toString().trim().equals(""))
				result = true;
			if(myRow.getCell(EN_CE_CODICE_ATECUE)==null || myRow.getCell(EN_CE_CODICE_ATECUE).toString().trim().equals(""))
				result = true;
		}
		else{
			throw new InvalidExcelException("Nello Sheet consegna dell'Energia il numero di righe esportate è diverso da quello importate");
		}

		return result;
	}

	private static int getLastCellNum(Row myRow) {
		int lastCellNum = 0;
		for (int i=0; i<=30; i++){
			Cell cell = myRow.getCell(i);
			if (cell==null || cell.toString().trim().equals("")){
				continue;
			}
			lastCellNum = i;
		}
		return lastCellNum;
	}

	public static String getStringValue (Row myRow, int position) {
		return getValueAsGenericString (myRow, position);
	}

	public static Date getDateValue (Row myRow, int position) {
		String val = getValueAsGenericString(myRow, position);
		try {
			return new SimpleDateFormat("dd/MM/yyyy").parse(val);
		} catch (ParseException e) {
			return null;
		}
	}

	public static Double getDoubleValue (Row myRow, int position) {
		String val = getValueAsGenericString(myRow, position);
		try {
			return Double.parseDouble(val);
		}
		catch (NumberFormatException e){
			return null;
		}
	}

	public static Integer getIntValue (Row myRow, int position) {
		String val = getValueAsGenericString(myRow, position);
		try {
			return Integer.parseInt(val);
		}
		catch (NumberFormatException e){
			return null;
		}
	}

	public static BigDecimal getBigDecimalValue (Row myRow, int position) {
		String val = getValueAsGenericString(myRow, position);
		try {
			return new BigDecimal(val);
		}
		catch (NumberFormatException e){
			return null;
		}
	}

	public static String getValueAsGenericString(Row myRow, int position) {
		Cell cell = myRow.getCell(position);
		if (cell==null){
			return "";
		}
		switch (cell.getCellType()){
		case Cell.CELL_TYPE_STRING:
			return cell.getStringCellValue();
		case Cell.CELL_TYPE_NUMERIC:
			if (DateUtil.isCellDateFormatted(cell)){
				Date data = cell.getDateCellValue();
				if (data==null){
					return "";
				}
				return new SimpleDateFormat("dd/MM/yyyy").format(data);
			}
			else {
				double num = cell.getNumericCellValue();
				if (Math.round(num) - num == 0)
					return Integer.toString((int)num);
				return Double.toString(num);
			}
		case Cell.CELL_TYPE_BLANK:
			if (DateUtil.isCellDateFormatted(cell)){
				Date data = cell.getDateCellValue();
				if (data==null){
					return "";
				}
				return new SimpleDateFormat("dd/MM/yyyy").format(data);
			}
			else {
				return cell.getStringCellValue();
			}
		}
		return cell.getStringCellValue();
	}

	private static void skip(Iterator<Row> rowsIter, int steps) {
		for (int i=0; i<steps; i++){
			rowsIter.next();
		}
	}

	public static boolean fileContainsFormula(UploadedFile file) throws IOException {
		try {
			Workbook wb = null;
			try {
				wb = new HSSFWorkbook(file.getInputstream());
			}
			catch (Exception e){
				wb = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(file.getInputstream());
			}
			int sheetNum = wb.getNumberOfSheets();
			for (int i=0; i<sheetNum; i++){
				Sheet sheet = wb.getSheetAt(i);
				Iterator<Row> rowIter = sheet.rowIterator();
				while (rowIter.hasNext()){
					Row row = rowIter.next();
					Iterator<Cell> cellIter = row.cellIterator();
					while (cellIter.hasNext()){
						Cell cell = cellIter.next();
						if (cell.getCellType()==Cell.CELL_TYPE_FORMULA){
							return true;
						}
					}
				}
			}
			return false;
		}
		finally {
			file.getInputstream().close();
		}
	}

	public static BigDecimal getBigDecimalCellValue(Row myRow,int position) {
		BigDecimal value = getBigDecimalValue(myRow,position);
		if (value == null)
			value = BigDecimal.ZERO;
		return value;
	}

	/**
	 * fileContainsNegativeNumbers : Metodo utilizzato per controllare la presenza di numeri negativi nelle celle di un foglio excel.
	 * 
	 * @author Fabio Ponte
	 * @param file : Il file excel caricato con i dati da importare.
	 * @param template : Il template del file di export.
	 * @return restituisce un boolean true se sono presenti numeri negativi altrimenti restituisce false
	 * @throws IOException
	 * @throws InvalidExcelException
	 */
	public static boolean fileContainsNegativeNumbers(UploadedFile file,XlsExportTemplate template) throws IOException,InvalidExcelException {
		try {
			Workbook wb = null;
			try {
				wb = new HSSFWorkbook(file.getInputstream());
			}
			catch(Exception e) {
				wb = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(file.getInputstream());
			}
			String filename = file.getFileName();
			String[] tokens = filename.split("_");
			String tipoImpianto = tokens[3];
			if(!tokens[3].equals("T") && !tokens[3].equals("X")) {
				throw new InvalidExcelException("Nome file ("+filename+") non valido.");
			}
			int sheetNum = wb.getNumberOfSheets();
			for(int i = 2; i < sheetNum; i++){
				Sheet sheet = wb.getSheetAt(i);
				Iterator<Row> rowIter = sheet.rowIterator();
				if(i == 4)
					skip(rowIter,1);
				else
					skip(rowIter,2);
				int indice = 0;
				while(rowIter.hasNext()) {
					indice++;
					Row row = rowIter.next();
					if(tipoImpianto.equals("T")) {
						if(i == 2) {
							if(!verifyIfRowProdEnergiaProdottaIsEmpty(row,indice,template)) {
				 				if((getBigDecimalCellValue(row, EN_PR_POTENZA_EFFICIENTE_LORDA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_PR_PRODUZIONE_LORDA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_PR_ENERGIA_ASSORBITA_PER_SA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_PR_DI_CUI_ASSORBITI)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_PR_ENERGIA_PRELEVATA_DALLA_RETE)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_PR_IMMESSA_SU_RETE_PUBBLICA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
							}
						}
						if(i == 3) {
							if(!verifyIfRowProdUtilizzoEnergiaIsEmpty(row,indice,template)) {
								if((getBigDecimalCellValue(row, EN_UE_NUMERO_UTENZE)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_UE_QUANTITA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
							}
						}
						if(i == 4) {
							if(!verifyIfRowProdCombustibiliUtilizzatiIsEmpty(row,indice,template)) {
				 				if((getBigDecimalCellValue(row, EN_CU_PCI)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_CU_CSE)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_CU_RENDIMENTO_CALDAIA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_CU_QUANTITA_IMPIEGATA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
							}
						}
						if(i == 5) {
							if(!verifyIfRowProdTUtilizziCaloreIsEmpty(row,indice,template)) {
				 				if((getBigDecimalCellValue(row, EN_UC_NUMERO_UTENZE)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_UC_QUANTITA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
							}
						}
						if(i == 6) {
							if(!verifyIfRowProdTCaloreDigestoreIsEmpty(row,indice,template)) {
				 				if((getBigDecimalCellValue(row, EN_CD_CALORE_DIGESTORE)).compareTo(BigDecimal.ZERO) < 0)
									return true;
							}
						}
					}
					else if(tipoImpianto.equals("X")) {
						if(i == 2) {
							if(!verifyIfRowProdEnergiaProdottaIsEmpty(row,indice,template)) {
				 				if((getBigDecimalCellValue(row, EN_PR_POTENZA_EFFICIENTE_LORDA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_PR_PRODUZIONE_LORDA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_PR_ENERGIA_ASSORBITA_PER_SA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_PR_DI_CUI_ASSORBITI)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_PR_ENERGIA_PRELEVATA_DALLA_RETE)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_PR_IMMESSA_SU_RETE_PUBBLICA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
							}
						}
						if(i == 3) {
							if(!verifyIfRowProdUtilizzoEnergiaIsEmpty(row,indice,template)) {
								if((getBigDecimalCellValue(row, EN_UE_NUMERO_UTENZE)).compareTo(BigDecimal.ZERO) < 0)
									return true;
				 				if((getBigDecimalCellValue(row, EN_UE_QUANTITA)).compareTo(BigDecimal.ZERO) < 0)
									return true;
							}
						}
						if(i == 4) {
							if(!verifyIfRowProdXPompaggiIsEmpty(row,indice,template)) {
				 				if((getBigDecimalCellValue(row, EN_PO_CONSUMO_POMPAGGIO)).compareTo(BigDecimal.ZERO) < 0)
									return true;
							}
						}
					}
				}
			}
			return false;
		}
		finally {
			file.getInputstream().close();
		}
	}
}
