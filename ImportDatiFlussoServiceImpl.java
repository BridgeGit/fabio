package it.terna.gstat.services.excelimport;

import it.terna.gstat.dao.LogElencoProcessiDAO;
import it.terna.gstat.dao.LogProcessiDAO;
import it.terna.gstat.dao.SysBatchDatiFlussoDAO;
import it.terna.gstat.dao.SysBatchStagingAreaDAO;
import it.terna.gstat.dao.TaUtenteEsternoDAO;
import it.terna.gstat.dao.TaUtenteInternoDAO;
import it.terna.gstat.dao.XlsCaloreDigestoreDAO;
import it.terna.gstat.dao.XlsCombustUtilizzatiDAO;
import it.terna.gstat.dao.XlsConsegnaEnergiaDAO;
import it.terna.gstat.dao.XlsEnergiaProdottaDAO;
import it.terna.gstat.dao.XlsExportTemplateDAO;
import it.terna.gstat.dao.XlsPompaggiDAO;
import it.terna.gstat.dao.XlsUtilizziCaloreDAO;
import it.terna.gstat.dao.XlsUtilizzoEnergiaDAO;
import it.terna.gstat.entities.LogElencoProcessi;
import it.terna.gstat.entities.LogProcessi;
import it.terna.gstat.entities.SysBatchDatiFlusso;
import it.terna.gstat.entities.SysBatchStagingArea;
import it.terna.gstat.entities.XlsCaloreDigestore;
import it.terna.gstat.entities.XlsCombustUtilizzati;
import it.terna.gstat.entities.XlsConsegnaEnergia;
import it.terna.gstat.entities.XlsEnergiaProdotta;
import it.terna.gstat.entities.XlsExportTemplate;
import it.terna.gstat.entities.XlsPompaggi;
import it.terna.gstat.entities.XlsUtilizziCalore;
import it.terna.gstat.entities.XlsUtilizzoEnergia;
import it.terna.gstat.excel.ExcelReader;
import it.terna.gstat.exceptions.DAOException;
import it.terna.gstat.exceptions.InvalidExcelException;
import it.terna.gstat.presentation.beans.session.SessionBean;
import it.terna.gstat.presentation.entity.ConvalidaWrapper;
import it.terna.gstat.presentation.entity.ProcessiWrapper;
import it.terna.gstat.presentation.entity.UserWrapper;
import it.terna.gstat.presentation.entity.WrapperUtils;
import it.terna.gstat.services.AbstractService;
import it.terna.gstat.services.congelamento.CongelamentoService;
import it.terna.gstat.services.stagingarea.NomeBatch;
import it.terna.gstat.services.statisticheAnnuali.ConvalidaFineInserimentoService;
import it.terna.gstat.utils.CompareUtils;
import it.terna.gstat.utils.DateUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.primefaces.model.UploadedFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service(value="ImportDatiFlussoService")
@Scope("request")
public class ImportDatiFlussoServiceImpl extends AbstractService implements ImportDatiFlussoService{

	@Autowired
	private SysBatchDatiFlussoDAO sysBatchDatiFlussoDAO;
	@Autowired
	private SysBatchStagingAreaDAO sysBatchStagingAreaDAO;	
	@Autowired
	private LogProcessiDAO logProcessiDAO;
	@Autowired
	private TaUtenteEsternoDAO utenteEsternoDAO;
	@Autowired
	private TaUtenteInternoDAO utenteInternoDAO;
	@Autowired
	private XlsEnergiaProdottaDAO energiaProdottaDAO;
	@Autowired
	private XlsUtilizzoEnergiaDAO utilizzoEnergiaDAO;
	@Autowired
	private XlsCombustUtilizzatiDAO combustUtilizzatiDAO;
	@Autowired
	private XlsUtilizziCaloreDAO utilizziCaloreDAO;
	@Autowired
	private XlsCaloreDigestoreDAO caloreDigestoreDAO;
	@Autowired
	private XlsPompaggiDAO pompaggiDAO;
	@Autowired
	private XlsExportTemplateDAO exportTemplateDAO;
	@Autowired
	private XlsConsegnaEnergiaDAO consegnaEnergiaDAO;
	@Autowired
	private LogProcessiDAO processiDAO;
	@Autowired
	private LogElencoProcessiDAO elencoProcessiDAO;
	@Autowired
	private ConvalidaFineInserimentoService convalidaFineInserimentoService;
	@Autowired
	private CongelamentoService congelamentoService;

	private XlsExportTemplate template = null;

	@SuppressWarnings("unchecked")
	@Override
	@Transactional
	public List<ProcessiWrapper> listProcessi(){
		List<LogProcessi> procList = processiDAO.getListInversa();
		return (List<ProcessiWrapper>) WrapperUtils.toWrappedList(ProcessiWrapper.class, LogProcessi.class, procList);
	}

	@SuppressWarnings("unchecked")
	@Override
	@Transactional
	public List<ProcessiWrapper> listProcessi(String codOeGstat, UserWrapper user){
		List<LogProcessi> procList = new ArrayList<LogProcessi>();
		if (user.getMatricola()!=null){
			procList = processiDAO.getListInversaProduttore(codOeGstat, user.getId(), 0);
		}
		else {
			procList = processiDAO.getListInversaProduttore(codOeGstat, 0, user.getId());
		}
		return (List<ProcessiWrapper>) WrapperUtils.toWrappedList(ProcessiWrapper.class, LogProcessi.class, procList);
	}

	@Override
	@Transactional
	public List<ProcessiWrapper> lastProcessoProduttore(String codOeGstat, UserWrapper user){
		List<LogProcessi> procList = new ArrayList<LogProcessi>();
//		if (user.getMatricola()!=null){
//			procList = processiDAO.getListInversaProduttore(codOeGstat, user.getId(), 0);
//		}
//		else {
//			procList = processiDAO.getListInversaProduttore(codOeGstat, 0, user.getId());
//		}
//		List<ProcessiWrapper> lst = new ArrayList<ProcessiWrapper>();
//		if (procList.size()>0){
//			lst.add(new ProcessiWrapper(procList.get(0)));
//		}
		procList = processiDAO.getListInversaProduttore(codOeGstat, 0, 0);
		List<ProcessiWrapper> lst = new ArrayList<ProcessiWrapper>();
		boolean annoCurrTermo = false;
		boolean annoPrecTermo = false;
		boolean annoCurrNoTermo = false;
		boolean annoPrecNoTermo = false;
		Calendar c = Calendar.getInstance();
		String annoCurr = Integer.toString(c.get(Calendar.YEAR));
		String annoPrec = Integer.toString(c.get(Calendar.YEAR)-1);
		for (LogProcessi p: procList){
			String filename = p.getSysBatchDatiFlussos().iterator().next().getNomefile();
			if (!annoCurrTermo && filename.startsWith(annoCurr) && filename.contains("_T_")){
				lst.add(new ProcessiWrapper(p));
				annoCurrTermo = true;
			}
			if (!annoPrecTermo && filename.startsWith(annoPrec) && filename.contains("_T_")){
				lst.add(new ProcessiWrapper(p));
				annoPrecTermo = true;
			}
			if (!annoCurrNoTermo && filename.startsWith(annoCurr) && filename.contains("_X_")){
				lst.add(new ProcessiWrapper(p));
				annoCurrNoTermo = true;
			}
			if (!annoPrecNoTermo && filename.startsWith(annoPrec) && filename.contains("_X_")){
				lst.add(new ProcessiWrapper(p));
				annoPrecNoTermo = true;
			}
		}
		return lst;
	}

	@Override
	@Transactional
	public List<ProcessiWrapper> lastProcessoDistributore(String codOeGstat, UserWrapper user){
		List<LogProcessi> procList = new ArrayList<LogProcessi>();
//		if (user.getMatricola()!=null){
//			procList = processiDAO.getListInversaDistributore(codOeGstat, user.getId(), 0);
//		}
//		else {
//			procList = processiDAO.getListInversaDistributore(codOeGstat, 0, user.getId());
//		}
//		List<ProcessiWrapper> lst = new ArrayList<ProcessiWrapper>();
//		if (procList.size()>0){
//			lst.add(new ProcessiWrapper(procList.get(0)));
//		}
		procList = processiDAO.getListInversaDistributore(codOeGstat, 0, 0);
		List<ProcessiWrapper> lst = new ArrayList<ProcessiWrapper>();
		boolean annoCurr = false;
		boolean annoPrec = false;
		Calendar c = Calendar.getInstance();
		String annoCurrStr = Integer.toString(c.get(Calendar.YEAR));
		String annoPrecStr = Integer.toString(c.get(Calendar.YEAR)-1);
		for (LogProcessi p: procList){
			String filename = p.getSysBatchDatiFlussos().iterator().next().getNomefile();
			if (!annoCurr && filename.startsWith(annoCurrStr)){
				lst.add(new ProcessiWrapper(p));
				annoCurr = true;
			}
			if (!annoPrec && filename.startsWith(annoPrecStr)){
				lst.add(new ProcessiWrapper(p));
				annoPrec = true;
			}
		}
		return lst;
	}

	@Override
	@Transactional (rollbackFor=Exception.class)
	public void importDatiFlussoProd(UploadedFile file, Short anno, SessionBean session) throws IOException, InvalidExcelException, DAOException {
		String filename = file.getFileName();
		//se il nome mi arriva completo di path, glielo tolgo
		String[] path = filename.split("/");
		filename = path[path.length-1];
		path = filename.split("\\\\");
		filename = path[path.length-1];

		//faccio un controllo con regex
		String regex = "[0-9]{4,4}_P_[A-Za-z0-9]*_[TX]_[0-9]{8,8}\\.xls[x]{0,1}";
		if (!Pattern.matches(regex , filename)){
			throw new InvalidExcelException("Nome file ("+filename+") non valido.");
		}

		filename = filename.split("\\.")[0];

		//Controllo che sia stato fatto prima l'export dell'xls
		template = exportTemplateDAO.getTemplateByNomeFile(filename);

		if(template==null)
		{
			throw new InvalidExcelException("Impossibile importare il file ("+filename+") in quanto non risulta esportato.");
		}
		
		//controllo che il file non contenga formule
		if (ExcelReader.fileContainsFormula(file)){
			throw new InvalidExcelException("Impossibile importare il file ("+filename+") in quanto contiene formule.");
		}

		/**
	 	* Esegue un controllo sul file che non contenga numeri negativi.
	 	*/
		if(ExcelReader.fileContainsNegativeNumbers(file,template)) {
			throw new InvalidExcelException("Impossibile importare il file ("+filename+") in quanto contiene numeri negativi.");
		}

		//ANOMALIA 561
		//Controllo che l'xls non sia già dato in pasto al batch
		ConvalidaWrapper convalida = convalidaFineInserimentoService.getConvalidaByCodOeGstatAnnoProd(session.getSelectedOperatore().getCodOeGstat(), anno);
		if(convalida!=null  && convalida.getConvalidaFineInserimento() != null)
		{
			throw new InvalidExcelException("Impossibile importare il file ("+filename+") in quanto è già stata effettuata la convalida fine inserimento per l'anno specificato.");
		}	

		String[] tokens = filename.split("_");
		if (tokens.length!=5){
			throw new InvalidExcelException("Nome file ("+filename+") non valido.");
		}

		if(session.getSelectedOperatore() != null && 
				session.getSelectedOperatore().getPartitaIva() != null && 
				!tokens[2].equalsIgnoreCase(session.getSelectedOperatore().getPartitaIva())){
			throw new InvalidExcelException("L'operatore "+ tokens[2] + " indicato nel file non corrisponde all'OE loggato.");
		}
		
		//controllo che l'anno del nomefile corrisponda a quello specificato in interfaccia
		if (!tokens[0].equals(Short.toString(anno))){
			throw new InvalidExcelException("L'anno specificato nel nome file ("+filename+") non corrisponde con quello selezionato ("+anno+").");
		}

		//controllo che sia un anno congelato
		if (DateUtils.isCongelatoDF(Integer.parseInt(tokens[0]), congelamentoService)){
			throw new InvalidExcelException("L'anno specificato nel nome file ("+filename+") corrisponde ad un anno congelato");
		}	
		
		//switcha in base al nome del file
		if (!tokens[1].equals("P")){
			throw new InvalidExcelException("Nome file ("+filename+") non valido.");
		}
		if (!tokens[3].equals("T") && !tokens[3].equals("X")){
			throw new InvalidExcelException("Nome file ("+filename+") non valido.");
		}

		//ANOMALIA 548
		boolean flagUpdateLog = true;
		boolean flagUpdateSys = true;
		SysBatchDatiFlusso df = sysBatchDatiFlussoDAO.getByNomeFile(filename);
		if(df!=null && df.getNumRecordLetti()==null){
			LogProcessi lp = df.getLogProcessi();
			if(lp != null && lp.getDataFineProcesso() == null && lp.getDataFineProcesso() == null){
				lp.setDataRichiesta(new Date());

				logProcessiDAO.update(lp);
				logProcessiDAO.refresh(lp);
				flagUpdateLog = false;
			}

			df.setDataRichiesta(new Date());
			df.setDatiInput(sysBatchDatiFlussoDAO.createBlob(file.getContents()));
			if (session.getUser().getMatricola()!=null){
				df.setTaUtenteInterno(utenteInternoDAO.getById(session.getUser().getId()));
			}
			else {
				df.setTaUtenteEsterno(utenteEsternoDAO.getById(session.getUser().getId()));
			}

			sysBatchDatiFlussoDAO.update(df);
			sysBatchDatiFlussoDAO.refresh(df);

			energiaProdottaDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			utilizzoEnergiaDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			combustUtilizzatiDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			utilizziCaloreDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			caloreDigestoreDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());

			energiaProdottaDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			utilizzoEnergiaDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			pompaggiDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());

			flagUpdateSys = false;
		}

		if(flagUpdateLog || flagUpdateSys){
			LogProcessi pr = new LogProcessi();

			if(flagUpdateLog){
				pr.setAnno(anno);
				pr.setDataRichiesta(new Date());
				pr.setPrgAnnoBatch(logProcessiDAO.getNextPrgAnno(anno));
				pr.setNomeProcesso("IMPORT_DATI_FLUSSO_EXCEL_PRODUTTORE");

				LogElencoProcessi elencoProcessi = elencoProcessiDAO.getLogElencoProcessiByNomeProcesso("IMPORT_DATI_FLUSSO_EXCEL_PRODUTTORE");
				if(elencoProcessi!=null)
					pr.setLogElencoProcessi(elencoProcessi);
				else
					throw new DAOException("LogElencoProcessi 'IMPORT_DATI_FLUSSO_EXCEL_PRODUTTORE' non trovato");

				logProcessiDAO.save(pr);
				logProcessiDAO.refresh(pr);
			}

			if(flagUpdateSys){
				df = new SysBatchDatiFlusso();
				df.setCodOeGstat(session.getSelectedOperatore().getCodOeGstat());
				df.setDataRichiesta(new Date());
				df.setDatiInput(sysBatchDatiFlussoDAO.createBlob(file.getContents()));
				df.setLogProcessi(pr);
				df.setAnno(anno);
				df.setDataRichiesta(new Date());
				df.setNomefile(filename);
				if (session.getUser().getMatricola()!=null){
					df.setTaUtenteInterno(utenteInternoDAO.getById(session.getUser().getId()));
				}
				else {
					df.setTaUtenteEsterno(utenteEsternoDAO.getById(session.getUser().getId()));
				}

				sysBatchDatiFlussoDAO.save(df);
				sysBatchDatiFlussoDAO.refresh(df);
			}
		}

		if (tokens[3].equals("T")){
			//termico
			ArrayList<XlsEnergiaProdotta> tabEnergiaProdotta = ExcelReader.parsaProdTEnergiaProdotta(file, df, template);
			ArrayList<XlsUtilizzoEnergia> tabUtilizzoEnergia = ExcelReader.parsaProdTUtilizzoEnergia(file, df, template);
			ArrayList<XlsCombustUtilizzati> tabCombustibiliUtilizzati = ExcelReader.parsaProdTCombustibiliUtilizzati(file, df, template);
			ArrayList<XlsUtilizziCalore> tabUtilizziCalore = ExcelReader.parsaProdTUtilizziCalore(file, df, template);
			ArrayList<XlsCaloreDigestore> tabCaloreDigestore = ExcelReader.parsaProdTCaloreDigestore(file, df, template);

			energiaProdottaDAO.batchInsert(tabEnergiaProdotta);
			utilizzoEnergiaDAO.batchInsert(tabUtilizzoEnergia);
			combustUtilizzatiDAO.batchInsert(tabCombustibiliUtilizzati);
			utilizziCaloreDAO.batchInsert(tabUtilizziCalore);
			caloreDigestoreDAO.batchInsert(tabCaloreDigestore);

		}
		else if (tokens[3].equals("X")){
			//non termico
			ArrayList<XlsEnergiaProdotta> tabEnergiaProdotta = ExcelReader.parsaProdTEnergiaProdotta(file, df, template);
			ArrayList<XlsUtilizzoEnergia> tabUtilizzoEnergia = ExcelReader.parsaProdTUtilizzoEnergia(file, df, template);
			ArrayList<XlsPompaggi> tabPompaggi = ExcelReader.parsaProdXPompaggi(file, df, template);

			energiaProdottaDAO.batchInsert(tabEnergiaProdotta);
			utilizzoEnergiaDAO.batchInsert(tabUtilizzoEnergia);
			pompaggiDAO.batchInsert(tabPompaggi);

		}
	}

	@Override
	@Transactional (rollbackFor=Exception.class)
	public void importDatiFlussoFoto(UploadedFile file, Integer anno, UserWrapper user) throws IOException, InvalidExcelException, DAOException {
		String filename = file.getFileName();
		//se il nome mi arriva completo di path, glielo tolgo
		String[] path = filename.split("/");
		filename = path[path.length-1];
		path = filename.split("\\\\");
		filename = path[path.length-1];

		String dateInString=filename.substring(41, 49);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
	
			Date date=new Date();
			try {
				date = formatter.parse(dateInString);
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		//faccio un controllo con regex
		String regex = "[0-9]{4,4}_CaricamentoDatiFlussoFotovoltaico_I_[0-9]{8,8}\\.xls[x]{0,1}";
		if (!Pattern.matches(regex , filename)){
			throw new InvalidExcelException("Nome file ("+filename+") non valido.");
		}
		
		//controllo che il file non contenga formule
		if (ExcelReader.fileContainsFormula(file)){
			throw new InvalidExcelException("Impossibile importare il file ("+filename+") in quanto contiene formule.");
		}

		filename = filename.split("\\.")[0];

		//ANOMALIA 561
		//Controllo che l'xls non sia già dato in pasto al batch

		String[] tokens = filename.split("_");

		if(!CompareUtils.same(date, DateUtils.oggi()))	
			throw new InvalidExcelException("Il nome del file non contiene la data odierna.");
		//controllo che l'anno del nomefile corrisponda a quello specificato in interfaccia
		if (!tokens[0].equals(Integer.toString(anno))){
			throw new InvalidExcelException("L'anno specificato nel nome file ("+filename+") non corrisponde con quello selezionato ("+anno+").");
		}

		//controllo che sia un anno congelato
		if (DateUtils.isCongelatoDF(Integer.parseInt(tokens[0]), congelamentoService)){
			throw new InvalidExcelException("L'anno specificato nel nome file ("+filename+") corrisponde ad un anno congeleato");
		}	
		
		//ANOMALIA 548
		boolean flagUpdateLog = true;
		boolean flagUpdateSys = true;
		SysBatchDatiFlusso df = sysBatchDatiFlussoDAO.getByNomeFile(filename);
		if(df!=null && df.getNumRecordLetti()==null){
			LogProcessi lp = df.getLogProcessi();
			if(lp != null && lp.getDataFineProcesso() == null){
				lp.setDataRichiesta(new Date());

				logProcessiDAO.update(lp);
				logProcessiDAO.refresh(lp);
				flagUpdateLog = false;
			}

			df.setDataRichiesta(new Date());
			df.setDatiInput(sysBatchDatiFlussoDAO.createBlob(file.getContents()));
			if (user.getMatricola()!=null){
				df.setTaUtenteInterno(utenteInternoDAO.getById(user.getId()));
			}
			else {
				df.setTaUtenteEsterno(utenteEsternoDAO.getById(user.getId()));
			}

			sysBatchDatiFlussoDAO.update(df);
			sysBatchDatiFlussoDAO.refresh(df);

			energiaProdottaDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			utilizzoEnergiaDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			combustUtilizzatiDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			utilizziCaloreDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			caloreDigestoreDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());

			energiaProdottaDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			utilizzoEnergiaDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			pompaggiDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());

			flagUpdateSys = false;
		}

		if(flagUpdateLog || flagUpdateSys){
			LogProcessi pr = new LogProcessi();

			if(flagUpdateLog){
				pr.setAnno(anno.shortValue());
				pr.setDataRichiesta(new Date());
				pr.setPrgAnnoBatch(logProcessiDAO.getNextPrgAnno(anno.shortValue()));
				pr.setNomeProcesso(NomeBatch.STAGING_AREA_CARICAMENTO_FLUSSO_FOTO.toString());

				LogElencoProcessi elencoProcessi = elencoProcessiDAO.getLogElencoProcessiByNomeProcesso(NomeBatch.STAGING_AREA_CARICAMENTO_FLUSSO_FOTO.toString());
				if(elencoProcessi!=null)
					pr.setLogElencoProcessi(elencoProcessi);
				else
					throw new DAOException("LogElencoProcessi '"+NomeBatch.STAGING_AREA_CARICAMENTO_FLUSSO_FOTO.toString()+"' non trovato");

				logProcessiDAO.save(pr);
				logProcessiDAO.refresh(pr);
			}

			if(flagUpdateSys){
				df = new SysBatchDatiFlusso();
				df.setCodOeGstat("");
				df.setDataRichiesta(new Date());
				df.setDatiInput(sysBatchDatiFlussoDAO.createBlob(file.getContents()));
				df.setLogProcessi(pr);
				df.setAnno(anno.shortValue());
				df.setDataRichiesta(new Date());
				df.setNomefile(filename);
				if (user.getMatricola()!=null){
					df.setTaUtenteInterno(utenteInternoDAO.getById(user.getId()));
				}
				else {
					df.setTaUtenteEsterno(utenteEsternoDAO.getById(user.getId()));
				}

				sysBatchDatiFlussoDAO.save(df);
				sysBatchDatiFlussoDAO.refresh(df);
			}
		}
		
		//non termico
		ArrayList<XlsEnergiaProdotta> tabEnergiaProdotta = ExcelReader.parsaProdTEnergiaProdotta(file, df, template);
		ArrayList<XlsUtilizzoEnergia> tabUtilizzoEnergia = ExcelReader.parsaProdTUtilizzoEnergia(file, df, template);
		ArrayList<XlsPompaggi> tabPompaggi = ExcelReader.parsaProdXPompaggi(file, df, template);

		energiaProdottaDAO.batchInsert(tabEnergiaProdotta);
		utilizzoEnergiaDAO.batchInsert(tabUtilizzoEnergia);
		pompaggiDAO.batchInsert(tabPompaggi);

	}
	
	@Override
	@Transactional (rollbackFor=Exception.class)
	public void exportDatiFlussoFoto(String filename, Integer anno, UserWrapper user) throws IOException, InvalidExcelException, DAOException {
		
		//ANOMALIA 548
		boolean flagUpdateLog = true;
		boolean flagUpdateSys = true;
		SysBatchStagingArea df = sysBatchStagingAreaDAO.getByNomeFile(filename);
		if(df!=null && df.getNumRecordLetti()==null){
			LogProcessi lp = df.getLogProcessi();
			if(lp != null && lp.getDataFineProcesso() == null){
				lp.setDataRichiesta(new Date());

				logProcessiDAO.update(lp);
				logProcessiDAO.refresh(lp);
				flagUpdateLog = false;
			}

			df.setDataRichiesta(new Date());
			//df.setDatiInput(sysBatchDatiFlussoDAO.createBlob(file.getContents()));
			if (user.getMatricola()!=null){
				df.setTaUtenteInterno(utenteInternoDAO.getById(user.getId()));
			}

			sysBatchStagingAreaDAO.update(df);
			sysBatchStagingAreaDAO.refresh(df);

			flagUpdateSys = false;
		}

		if(flagUpdateLog || flagUpdateSys){
			LogProcessi pr = new LogProcessi();

			if(flagUpdateLog){
				pr.setAnno(anno.shortValue());
				pr.setDataRichiesta(new Date());
				pr.setPrgAnnoBatch(logProcessiDAO.getNextPrgAnno(anno.shortValue()));
				pr.setNomeProcesso(NomeBatch.STAGING_AREA_CARICAMENTO_FLUSSO_FOTO_EXPORT.toString());

				LogElencoProcessi elencoProcessi = elencoProcessiDAO.getLogElencoProcessiByNomeProcesso(NomeBatch.STAGING_AREA_CARICAMENTO_FLUSSO_FOTO_EXPORT.toString());
				if(elencoProcessi!=null)
					pr.setLogElencoProcessi(elencoProcessi);
				else
					throw new DAOException("LogElencoProcessi '"+NomeBatch.STAGING_AREA_CARICAMENTO_FLUSSO_FOTO.toString()+"' non trovato");

				logProcessiDAO.save(pr);
				logProcessiDAO.refresh(pr);
			}

			if(flagUpdateSys){
				df = new SysBatchStagingArea();
				df.setDataRichiesta(new Date());
				//df.setDatiInput(sysBatchDatiFlussoDAO.createBlob(file.getContents()));
				df.setLogProcessi(pr);
				df.setAnno(anno.shortValue());
				df.setDataRichiesta(new Date());
				df.setNomefile(filename);
				if (user.getMatricola()!=null){
					df.setTaUtenteInterno(utenteInternoDAO.getById(user.getId()));
				}

				sysBatchStagingAreaDAO.save(df);
				sysBatchStagingAreaDAO.refresh(df);
			}
		}

	}
	
	@Override
	@Transactional (rollbackFor=Exception.class)
	public void importDatiFlussoDistr(UploadedFile file, Short anno, SessionBean session) throws IOException, InvalidExcelException, DAOException {
		String filename = file.getFileName();
		//se il nome mi arriva completo di path, glielo tolgo
		String[] path = filename.split("/");
		filename = path[path.length-1];
		path = filename.split("\\\\");
		filename = path[path.length-1];

		//faccio un controllo con regex
		String regex = "[0-9]{4,4}_[ASMB]_D_[A-Za-z0-9]*_[0-9]{8,8}\\.xls[x]{0,1}";
		if (!Pattern.matches(regex , filename)){
			throw new InvalidExcelException("Nome file ("+filename+") non valido.");
		}

		filename = filename.split("\\.")[0];

		//Controllo che sia stato fatto prima l'export dell'xls
		template = exportTemplateDAO.getTemplateByNomeFile(filename);
		if(template==null)
		{
			throw new InvalidExcelException("Impossibile importare il file ("+filename+") in quanto non risulta esportato.");
		}
		
		//controllo che il file non contenga formule
		if (ExcelReader.fileContainsFormula(file)){
			throw new InvalidExcelException("Impossibile importare il file ("+filename+") in quanto contiene formule.");
		}

		//ANOMALIA 561
		//Controllo che l'xls non sia già dato in pasto al batch
		ConvalidaWrapper convalida = convalidaFineInserimentoService.getConvalidaByCodOeGstatAnnoDist(session.getSelectedOperatore().getCodOeGstat(), anno);
		if(convalida!=null  && convalida.getConvalidaFineInserimento() != null)
		{
			throw new InvalidExcelException("Impossibile importare il file ("+filename+") in quanto è già stata effettuata la convalida fine inserimento per l'anno specificato.");
		}	

		String[] tokens = filename.split("_");
		if (tokens.length!=5){
			throw new InvalidExcelException("Nome file ("+filename+") non valido.");
		}

		if(session.getSelectedOperatore() != null && 
				session.getSelectedOperatore().getPartitaIva() != null && 
				!tokens[3].equalsIgnoreCase(session.getSelectedOperatore().getPartitaIva())){
			throw new InvalidExcelException("L'operatore "+ tokens[3] + " indicato nel file non corrisponde all'OE loggato.");
		}
		
		//controllo che l'anno del nomefile corrisponda a quello specificato in interfaccia
		if (!tokens[0].equals(Short.toString(anno))){
			throw new InvalidExcelException("L'anno specificato nel nome file ("+filename+") non corrisponde con quello selezionato ("+anno+").");
		}	
		
		//controllo che sia un anno congelato
		if (DateUtils.isCongelatoDF(Integer.parseInt(tokens[0]), congelamentoService)){
			throw new InvalidExcelException("L'anno specificato nel nome file ("+filename+") corrisponde ad un anno congeleato");
		}	

		//ANOMALIA 548
		boolean flagUpdateLog = true; 
		boolean flagUpdateSys = true;
		SysBatchDatiFlusso df = sysBatchDatiFlussoDAO.getByNomeFile(filename);
		if(df!=null && df.getNumRecordLetti()==null){
			LogProcessi lp = df.getLogProcessi();
			if(lp != null && lp.getDataFineProcesso() == null && lp.getDataInizioProcesso() == null){
				lp.setDataRichiesta(new Date());

				logProcessiDAO.update(lp);
				logProcessiDAO.refresh(lp);
				flagUpdateLog = false;
			}

			df.setDataRichiesta(new Date());
			df.setDatiInput(sysBatchDatiFlussoDAO.createBlob(file.getContents()));
			if (session.getUser().getMatricola()!=null){
				df.setTaUtenteInterno(utenteInternoDAO.getById(session.getUser().getId()));
			}
			else {
				df.setTaUtenteEsterno(utenteEsternoDAO.getById(session.getUser().getId()));
			}

			sysBatchDatiFlussoDAO.update(df);
			sysBatchDatiFlussoDAO.refresh(df);
			
			consegnaEnergiaDAO.deleteAllByIdBatchDatiFlusso(df.getIdBatchDatiFlusso());
			
			flagUpdateSys = false;
		}

		if(flagUpdateLog || flagUpdateSys){
			LogProcessi pr = new LogProcessi();

			if(flagUpdateLog){
				pr.setAnno(anno);
				pr.setDataRichiesta(new Date());
				pr.setPrgAnnoBatch(logProcessiDAO.getNextPrgAnno(anno));
				pr.setNomeProcesso("IMPORT_DATI_FLUSSO_EXCEL_DISTRIBUTORE");

				LogElencoProcessi elencoProcessi = elencoProcessiDAO.getLogElencoProcessiByNomeProcesso("IMPORT_DATI_FLUSSO_EXCEL_DISTRIBUTORE");
				if(elencoProcessi!=null)
					pr.setLogElencoProcessi(elencoProcessi);
				else
					throw new DAOException("LogElencoProcessi 'IMPORT_DATI_FLUSSO_EXCEL_DISTRIBUTORE' non trovato");

				logProcessiDAO.save(pr);
				logProcessiDAO.refresh(pr);
			}

			if(flagUpdateSys){
				df = new SysBatchDatiFlusso();
				df.setCodOeGstat(session.getSelectedOperatore().getCodOeGstat());
				df.setDataRichiesta(new Date());
				df.setDatiInput(sysBatchDatiFlussoDAO.createBlob(file.getContents()));
				df.setLogProcessi(pr);
				df.setAnno(anno);
				df.setDataRichiesta(new Date());
				df.setNomefile(filename);
				if (session.getUser().getMatricola()!=null){
					df.setTaUtenteInterno(utenteInternoDAO.getById(session.getUser().getId()));
				}
				else {
					df.setTaUtenteEsterno(utenteEsternoDAO.getById(session.getUser().getId()));
				}

				sysBatchDatiFlussoDAO.save(df);
				sysBatchDatiFlussoDAO.update(df);
			}

		}

			ArrayList<XlsConsegnaEnergia> tabConsegnaEnergia = ExcelReader.parsaDistrConsegnaEnergia(file, df, template);
			consegnaEnergiaDAO.batchInsert(tabConsegnaEnergia);
		}

		@Override
		@Transactional
		public void export(UserWrapper user, String codOeGstat, short anno,
				String filename, ByteArrayOutputStream out, int[] numRighe,
				int[] numColonne, String granularita) throws DAOException {

			XlsExportTemplate e = new XlsExportTemplate();
			e.setAnno(anno);
			e.setCodOeGstat(codOeGstat);
			e.setContenutoFile(exportTemplateDAO.createBlob(out.toByteArray()));
			e.setDataRichiesta(new Date());
			e.setNomefile(filename);
			e.setGranularita(granularita);
			if (user.getMatricola()!=null){
				e.setTaUtenteInterno(utenteInternoDAO.getById(user.getId()));
			}
			else {
				e.setTaUtenteEsterno(utenteEsternoDAO.getById(user.getId()));
			}

			int[] numrow = new int[10];
			for (int i=0; i<numRighe.length; i++){
				numrow[i] = numRighe[i];
			}
			int[] numcol = new int[10];
			for (int i=0; i<numColonne.length; i++){
				numcol[i] = numColonne[i];
			}
			e.setNumRigheSht1(numrow[0]);
			e.setNumRigheSht2(numrow[1]);
			e.setNumRigheSht3(numrow[2]);
			e.setNumRigheSht4(numrow[3]);
			e.setNumRigheSht5(numrow[4]);
			e.setNumRigheSht6(numrow[5]);
			e.setNumRigheSht7(numrow[6]);
			e.setNumRigheSht8(numrow[7]);
			e.setNumRigheSht9(numrow[8]);
			e.setNumRigheSht10(numrow[9]);
			e.setNumColonneSht1(numcol[0]);
			e.setNumColonneSht2(numcol[1]);
			e.setNumColonneSht3(numcol[2]);
			e.setNumColonneSht4(numcol[3]);
			e.setNumColonneSht5(numcol[4]);
			e.setNumColonneSht6(numcol[5]);
			e.setNumColonneSht7(numcol[6]);
			e.setNumColonneSht8(numcol[7]);
			e.setNumColonneSht9(numcol[8]);
			e.setNumColonneSht10(numcol[9]);

			exportTemplateDAO.save(e);

		}

		@Override
		@Transactional
		public byte[] getExcelLog(ProcessiWrapper p) throws SQLException {
			SysBatchDatiFlusso batch = sysBatchDatiFlussoDAO.getByIdProcesso(p.getPrgProcesso());
			if (batch!=null && batch.getDatiOutput()!=null){
				return batch.getDatiOutput().getBytes(1, (int)batch.getDatiOutput().length());
			}
			return null;
		}
		
		public void importDatiFlussoCaricManuali(UploadedFile file, Short anno){
			
		}

	}
