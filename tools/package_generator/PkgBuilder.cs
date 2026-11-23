// Generates ETL_BATCH_SQL_STG.dtsx (SSIS 2019 object model) WITHOUT connecting to any database.
// Column metadata is taken from the verified Phase A results.
// Load policy: Key = BatchNO (global), new rows INSERT, unchanged SKIP, changed -> CSV only (P1).
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using RW = Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask;

public class ColDef
{
    public string Src; public string Tgt; public RW.DataType Type;
    public int Len; public int Prec; public int Scale; public int CodePage;
    public ColDef(string src, string tgt, RW.DataType type, int len, int prec, int scale, int cp)
    { Src = src; Tgt = tgt; Type = type; Len = len; Prec = prec; Scale = scale; CodePage = cp; }
    public bool IsWStr { get { return Type == RW.DataType.DT_WSTR; } }
    public bool IsStr { get { return Type == RW.DataType.DT_STR; } }
}

public static class PkgBuilder
{
    static Application app = new Application();
    public static StringBuilder Log = new StringBuilder();

    public static List<ColDef> Cols()
    {
        var W = RW.DataType.DT_WSTR; var N = RW.DataType.DT_NUMERIC; var D = RW.DataType.DT_DBTIMESTAMP; var S = RW.DataType.DT_STR;
        return new List<ColDef> {
            new ColDef("BatchDate","BATCHDATE",D,0,0,0,0),
            new ColDef("OrderFullNo","ORDERFULLNO",W,100,0,0,0),
            new ColDef("UnitName","UNITNAME",W,100,0,0,0),
            new ColDef("ColorName","COLORNAME",W,150,0,0,0),
            new ColDef("BatchNO","BATCHNO",W,50,0,0,0),
            new ColDef("FabricDia","FABRICDIA",W,50,0,0,0),
            new ColDef("FabricGSM","FABRICGSM",W,50,0,0,0),
            new ColDef("OtherQty","OTHERQTY",N,0,18,4,0),
            new ColDef("Rolls","ROLLS",N,0,18,4,0),
            new ColDef("BuyerShortName","BUYERSHORTNAME",W,150,0,0,0),
            new ColDef("Item","ITEM",W,150,0,0,0),
            new ColDef("OrderCustomerType","ORDERCUSTOMERTYPE",W,50,0,0,0),
            new ColDef("BatchQty","BATCHQTY",N,0,18,4,0),
            new ColDef("BatchGreyQty","BATCHGREYQTY",N,0,18,4,0),
            new ColDef("CustomerTypeQty","CUSTOMERTYPEQTY",N,0,18,4,0),
            new ColDef("FabricTypeName","FABRICTYPE",W,150,0,0,0),
            new ColDef("DyeingStartDate","DYEINGSTARTDATE",D,0,0,0,0),
            new ColDef("TimelyBatchOK","TIMELYBATCHOK",S,1,0,0,1252),
            new ColDef("GreyReceiveTypeName","GREYRECEIVETYPENAME",W,150,0,0,0),
            new ColDef("PrdFloor","PRDFLOOR",W,100,0,0,0)
        };
    }

    static string CreationName(string friendly)
    {
        foreach (PipelineComponentInfo i in app.PipelineComponentInfos)
            if (i.Name == friendly) return i.CreationName;
        throw new Exception("Pipeline component not found: " + friendly);
    }

    static IDTSComponentMetaData100 AddComp(MainPipe pipe, string friendly, string name, out CManagedComponentWrapper w)
    {
        IDTSComponentMetaData100 m = pipe.ComponentMetaDataCollection.New();
        m.ComponentClassID = CreationName(friendly);
        w = m.Instantiate();
        w.ProvideComponentProperties();
        m.Name = name;
        return m;
    }

    static void SetConn(IDTSComponentMetaData100 m, ConnectionManager cm)
    {
        m.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(cm);
        m.RuntimeConnectionCollection[0].ConnectionManagerID = cm.ID;
    }

    static void Connect(MainPipe pipe, IDTSOutput100 o, IDTSComponentMetaData100 dest)
    {
        IDTSPath100 p = pipe.PathCollection.New();
        p.AttachPathAndPropagateNotifications(o, dest.InputCollection[0]);
    }

    static IDTSOutput100 OutByName(IDTSComponentMetaData100 m, string contains)
    {
        foreach (IDTSOutput100 o in m.OutputCollection) if (o.Name.Contains(contains)) return o;
        throw new Exception("Output '" + contains + "' not found on " + m.Name);
    }

    static IDTSOutput100 FirstNonErrorOutput(IDTSComponentMetaData100 m)
    {
        foreach (IDTSOutput100 o in m.OutputCollection) if (!o.IsErrorOut) return o;
        throw new Exception("No data output on " + m.Name);
    }

    static IDTSOutput100 ErrorOutput(IDTSComponentMetaData100 m)
    {
        foreach (IDTSOutput100 o in m.OutputCollection) if (o.IsErrorOut) return o;
        throw new Exception("No error output on " + m.Name);
    }

    static IDTSInputColumn100 UseCol(CManagedComponentWrapper w, IDTSInput100 input, string name)
    {
        IDTSVirtualInput100 vi = input.GetVirtualInput();
        foreach (IDTSVirtualInputColumn100 vc in vi.VirtualInputColumnCollection)
            if (vc.Name == name) return w.SetUsageType(input.ID, vi, vc.LineageID, DTSUsageType.UT_READONLY);
        throw new Exception("Virtual column '" + name + "' not found on input " + input.Name);
    }

    static IDTSComponentMetaData100 RowCount(MainPipe pipe, string name, string variable)
    {
        CManagedComponentWrapper w;
        IDTSComponentMetaData100 m = AddComp(pipe, "Row Count", name, out w);
        w.SetComponentProperty("VariableName", variable);
        return m;
    }

    // ---------------- Flat file (rejected / pending) ----------------
    static void FlatFile(Package pkg, MainPipe pipe, IDTSOutput100 upstream, string compName, string cmName, string stem, List<string> cols)
    {
        ConnectionManager ff = pkg.Connections.Add("FLATFILE");
        ff.Name = cmName;
        ff.Description = "Rows written for review: " + stem;
        ff.ConnectionString = @"E:\ETL_Logs\DBL_DIGITAL_TO_ORACLE\" + stem + ".csv";
        ff.SetExpression("ConnectionString", "@[$Package::LogFolder] + \"\\\\" + stem + "_\" + @[User::RunStamp] + \".csv\"");
        ff.Properties["Format"].SetValue(ff, "Delimited");
        ff.Properties["ColumnNamesInFirstDataRow"].SetValue(ff, true);
        ff.Properties["Unicode"].SetValue(ff, true);
        ff.Properties["TextQualifier"].SetValue(ff, "\"");
        ff.Properties["HeaderRowDelimiter"].SetValue(ff, "\r\n");

        CManagedComponentWrapper w;
        IDTSComponentMetaData100 dest = AddComp(pipe, "Flat File Destination", compName, out w);
        Connect(pipe, upstream, dest);
        IDTSInput100 input = dest.InputCollection[0];
        IDTSVirtualInput100 vi = input.GetVirtualInput();

        RW.IDTSConnectionManagerFlatFile100 ffi = (RW.IDTSConnectionManagerFlatFile100)ff.InnerObject;
        for (int i = 0; i < cols.Count; i++)
        {
            IDTSVirtualInputColumn100 vc = null;
            foreach (IDTSVirtualInputColumn100 c in vi.VirtualInputColumnCollection) if (c.Name == cols[i]) { vc = c; break; }
            if (vc == null) throw new Exception("Column " + cols[i] + " not available for " + compName);
            if (vc.DataType == RW.DataType.DT_STR) throw new Exception("DT_STR column " + cols[i] + " cannot go to a Unicode file");
            RW.IDTSConnectionManagerFlatFileColumn100 fc = ffi.Columns.Add();
            fc.ColumnType = "Delimited";
            fc.ColumnDelimiter = (i == cols.Count - 1) ? "\r\n" : ",";
            fc.DataType = vc.DataType;
            fc.DataPrecision = vc.Precision;
            fc.DataScale = vc.Scale;
            fc.MaximumWidth = vc.DataType == RW.DataType.DT_WSTR ? vc.Length : 50;
            fc.TextQualified = vc.DataType == RW.DataType.DT_WSTR;
            ((RW.IDTSName100)fc).Name = vc.Name;
        }

        SetConn(dest, ff);
        w.SetComponentProperty("Overwrite", true);
        w.AcquireConnections(null);
        w.ReinitializeMetaData();
        w.ReleaseConnections();

        vi = input.GetVirtualInput();
        foreach (string name in cols)
        {
            IDTSInputColumn100 ic = UseCol(w, input, name);
            IDTSExternalMetadataColumn100 ext = null;
            foreach (IDTSExternalMetadataColumn100 e in input.ExternalMetadataColumnCollection) if (e.Name == name) { ext = e; break; }
            if (ext == null) throw new Exception("External column " + name + " missing in " + compName);
            w.MapInputColumn(input.ID, ic.ID, ext.ID);
        }
    }

    // ---------------- Execute SQL helper ----------------
    static TaskHost SqlTask(Package pkg, string name, string desc, ConnectionManager cm, string sql, string[] resultVars)
    {
        TaskHost th = (TaskHost)pkg.Executables.Add("STOCK:SQLTask");
        th.Name = name; th.Description = desc;
        ExecuteSQLTask t = (ExecuteSQLTask)th.InnerObject;
        t.Connection = cm.ID;
        t.SqlStatementSource = sql;
        if (resultVars != null && resultVars.Length > 0)
        {
            t.ResultSetType = ResultSetType.ResultSetType_SingleRow;
            for (int i = 0; i < resultVars.Length; i++)
            {
                IDTSResultBinding b = t.ResultSetBindings.Add();
                b.ResultName = i.ToString();
                b.DtsVariableName = resultVars[i];
            }
        }
        return th;
    }

    static PrecedenceConstraint Link(Package pkg, Executable from, Executable to, string name, string expr)
    {
        PrecedenceConstraint pc = pkg.PrecedenceConstraints.Add(from, to);
        pc.Name = name;
        pc.Value = DTSExecResult.Success;
        if (expr != null) { pc.EvalOp = DTSPrecedenceEvalOp.ExpressionAndConstraint; pc.Expression = expr; }
        else pc.EvalOp = DTSPrecedenceEvalOp.Constraint;
        return pc;
    }

    public static string ChangeExpression(List<ColDef> cols)
    {
        var parts = new List<string>();
        foreach (ColDef c in cols)
        {
            if (c.Src == "BatchNO") continue;
            string s = "[" + c.Src + "]", t = "[TGT_" + c.Tgt + "]";
            if (c.IsWStr)
                parts.Add("(ISNULL(" + s + ") ? \"\" : " + s + ") != (ISNULL(" + t + ") ? \"\" : " + t + ")");
            else if (c.IsStr)
                parts.Add("(ISNULL([" + c.Src + "_W]) ? \"\" : [" + c.Src + "_W]) != (ISNULL(" + t + ") ? \"\" : " + t + ")");
            else
                parts.Add("(ISNULL(" + s + ") ? !ISNULL(" + t + ") : (ISNULL(" + t + ") ? TRUE : " + s + " != " + t + "))");
        }
        return string.Join(" || ", parts.ToArray());
    }

    public static void Build(string outPath)
    {
        List<ColDef> cols = Cols();
        Package pkg = new Package();
        pkg.Name = "ETL_BATCH_SQL_STG";
        pkg.Description = "DBL_DIGITAL.dbo.ETL_Batch_SQL_STG -> APPS.ETL_BATCH_SQL_STG. Key BatchNO; new=INSERT, unchanged=SKIP, changed=CSV report only (policy P1).";
        pkg.ProtectionLevel = DTSProtectionLevel.EncryptSensitiveWithUserKey;

        // Temporary package-level OLE DB connections (replaced by project CM references after save; never opened)
        ConnectionManager cmSql = pkg.Connections.Add("OLEDB");
        cmSql.Name = "TMP_CM_SQL";
        cmSql.ConnectionString = "Data Source=SQLSERVER_HOST;Initial Catalog=DBL_DIGITAL;Provider=SQLNCLI11.1;Integrated Security=SSPI;";
        ConnectionManager cmOra = pkg.Connections.Add("OLEDB");
        cmOra.Name = "TMP_CM_ORA";
        cmOra.ConnectionString = "Data Source=ORACLE_HOST:ORACLE_PORT/ORACLE_SERVICE;Provider=OraOLEDB.Oracle.1;";

        // Parameter + variables
        Parameter pLog = pkg.Parameters.Add("LogFolder", TypeCode.String);
        pLog.Value = @"E:\ETL_Logs\DBL_DIGITAL_TO_ORACLE";
        pLog.Description = "Folder for text log and rejected/pending CSV files (must exist at run time).";
        pLog.Required = false; pLog.Sensitive = false;

        string[] intVars = { "SourceRows", "SourceInvalidKeys", "SourceDupKeys", "RcSource", "RcInvalidKey", "RcNew", "RcUnchanged", "RcChanged", "RcRejected" };
        foreach (string v in intVars) pkg.Variables.Add(v, false, "User", 0);
        // Oracle COUNT(*) comes back as NUMBER; returned via TO_CHAR into String variables
        pkg.Variables.Add("TargetRowsBefore", false, "User", "0");
        pkg.Variables.Add("TargetDupKeys", false, "User", "0");
        pkg.Variables.Add("TargetRowsAfter", false, "User", "0");
        Variable stamp = pkg.Variables.Add("RunStamp", false, "User", "");
        stamp.EvaluateAsExpression = true;
        string st = "@[System::StartTime]";
        stamp.Expression = "(DT_WSTR,4)YEAR(" + st + ") + RIGHT(\"0\" + (DT_WSTR,2)MONTH(" + st + "),2) + RIGHT(\"0\" + (DT_WSTR,2)DAY(" + st + "),2) + \"_\" + RIGHT(\"0\" + (DT_WSTR,2)DATEPART(\"hh\"," + st + "),2) + RIGHT(\"0\" + (DT_WSTR,2)DATEPART(\"mi\"," + st + "),2) + RIGHT(\"0\" + (DT_WSTR,2)DATEPART(\"ss\"," + st + "),2)";

        // ---------------- Control flow ----------------
        TaskHost tSrc = SqlTask(pkg, "SQL_SOURCE_ROW_COUNT",
            "Read-only: source row count, NULL/blank BatchNO count, duplicate BatchNO groups",
            cmSql,
            "SELECT COUNT(*) AS SOURCE_ROWS,\r\n" +
            "       ISNULL(SUM(CASE WHEN BatchNO IS NULL OR LTRIM(RTRIM(BatchNO)) = N'' THEN 1 ELSE 0 END), 0) AS SOURCE_INVALID_KEYS,\r\n" +
            "       (SELECT COUNT(*) FROM (SELECT BatchNO FROM dbo.ETL_Batch_SQL_STG\r\n" +
            "                              WHERE BatchNO IS NOT NULL AND LTRIM(RTRIM(BatchNO)) <> N''\r\n" +
            "                              GROUP BY BatchNO HAVING COUNT(*) > 1) d) AS SOURCE_DUP_KEYS\r\n" +
            "FROM dbo.ETL_Batch_SQL_STG",
            new[] { "User::SourceRows", "User::SourceInvalidKeys", "User::SourceDupKeys" });

        TaskHost tOraPre = SqlTask(pkg, "ORACLE_TARGET_ROW_COUNT",
            "Read-only: target row count and duplicate BATCHNO groups before load",
            cmOra,
            // Oracle: a scalar subquery next to an aggregate without GROUP BY raises ORA-00937 -> two inline views
            "SELECT TO_CHAR(t.CNT) AS TARGET_ROWS, TO_CHAR(d.CNT) AS TARGET_DUP_KEYS\r\n" +
            "FROM (SELECT COUNT(*) AS CNT FROM APPS.ETL_BATCH_SQL_STG) t\r\n" +
            "CROSS JOIN (SELECT COUNT(*) AS CNT FROM (SELECT BATCHNO FROM APPS.ETL_BATCH_SQL_STG\r\n" +
            "            WHERE BATCHNO IS NOT NULL GROUP BY BATCHNO HAVING COUNT(*) > 1)) d",
            new[] { "User::TargetRowsBefore", "User::TargetDupKeys" });

        TaskHost tFailPre = SqlTask(pkg, "SQL_FAIL_PRE_VALIDATION",
            "Raises an error (no data change) when duplicate keys exist; nothing is loaded",
            cmSql, "SELECT 1", null);
        tFailPre.SetExpression("SqlStatementSource",
            "\"RAISERROR(N'ETL pre-validation failed: duplicate BatchNO groups in source = \" + (DT_WSTR,12)@[User::SourceDupKeys] + \", duplicate BATCHNO groups in Oracle target = \" + @[User::TargetDupKeys] + \". No rows were loaded.', 16, 1);\"");

        TaskHost tDft = (TaskHost)pkg.Executables.Add("STOCK:PipelineTask");
        tDft.Name = "DFT_SQL_TO_ORACLE_BATCH_STG";
        tDft.Description = "Source -> key check -> Lookup on BATCHNO -> INSERT new / SKIP unchanged / CSV changed";
        MainPipe pipe = (MainPipe)tDft.InnerObject;

        TaskHost tOraPost = SqlTask(pkg, "ORACLE_TARGET_ROW_COUNT_POST",
            "Read-only: target row count after load", cmOra,
            "SELECT TO_CHAR(COUNT(*)) AS TARGET_ROWS FROM APPS.ETL_BATCH_SQL_STG",
            new[] { "User::TargetRowsAfter" });

        TaskHost tFailPost = SqlTask(pkg, "SQL_FAIL_POST_VALIDATION",
            "Raises an error (no data change) when reconciliation fails or rows were rejected",
            cmSql, "SELECT 1", null);
        tFailPost.SetExpression("SqlStatementSource",
            "\"RAISERROR(N'ETL post-validation failed. SourceRows=\" + (DT_WSTR,12)@[User::SourceRows] + \", RcSource=\" + (DT_WSTR,12)@[User::RcSource] + \", RcInvalidKey=\" + (DT_WSTR,12)@[User::RcInvalidKey] + \", RcNew=\" + (DT_WSTR,12)@[User::RcNew] + \", RcUnchanged=\" + (DT_WSTR,12)@[User::RcUnchanged] + \", RcChanged=\" + (DT_WSTR,12)@[User::RcChanged] + \", RcRejected=\" + (DT_WSTR,12)@[User::RcRejected] + \", TargetRowsBefore=\" + @[User::TargetRowsBefore] + \", TargetRowsAfter=\" + @[User::TargetRowsAfter] + \". See CSV files in LogFolder.', 16, 1);\"");

        string preOk = "@[User::SourceDupKeys] == 0 && (DT_I4)@[User::TargetDupKeys] == 0";
        string postOk = "@[User::RcSource] == @[User::RcInvalidKey] + @[User::RcNew] + @[User::RcUnchanged] + @[User::RcChanged]" +
                        " && ((DT_I4)@[User::TargetRowsAfter] - (DT_I4)@[User::TargetRowsBefore]) == (@[User::RcNew] - @[User::RcRejected])" +
                        " && @[User::RcRejected] == 0 && @[User::RcInvalidKey] == 0";
        Link(pkg, tSrc, tOraPre, "PC_SRC_TO_ORA_PRE", null);
        Link(pkg, tOraPre, tDft, "PC_PRE_OK", preOk);
        Link(pkg, tOraPre, tFailPre, "PC_PRE_FAILED", "!(" + preOk + ")");
        Link(pkg, tDft, tOraPost, "PC_DFT_TO_ORA_POST", null);
        Link(pkg, tOraPost, tFailPost, "PC_POST_FAILED", "!(" + postOk + ")");

        // ---------------- Data flow ----------------
        CManagedComponentWrapper w;

        // Source
        IDTSComponentMetaData100 src = AddComp(pipe, "OLE DB Source", "OLE_SRC_ETL_BATCH_SQL_STG", out w);
        SetConn(src, cmSql);
        var sb = new StringBuilder("SELECT\r\n");
        for (int i = 0; i < cols.Count; i++)
        {
            ColDef c = cols[i];
            string expr = c.Type == RW.DataType.DT_DBTIMESTAMP
                ? "DATEADD(MILLISECOND, -DATEPART(MILLISECOND, " + c.Src + "), " + c.Src + ") AS " + c.Src
                : c.Src;
            sb.Append("    " + expr + (i < cols.Count - 1 ? ",\r\n" : "\r\n"));
        }
        sb.Append("FROM dbo.ETL_Batch_SQL_STG\r\n-- DATETIME truncated to whole seconds: Oracle DATE has no fractional seconds");
        w.SetComponentProperty("AccessMode", 2);
        w.SetComponentProperty("SqlCommand", sb.ToString());
        IDTSOutput100 so = FirstNonErrorOutput(src);
        IDTSOutput100 se = ErrorOutput(src);
        for (int i = 0; i < cols.Count; i++)
        {
            ColDef c = cols[i];
            IDTSExternalMetadataColumn100 ex = so.ExternalMetadataColumnCollection.New();
            ex.Name = c.Src; ex.DataType = c.Type; ex.Length = c.Len; ex.Precision = c.Prec; ex.Scale = c.Scale; ex.CodePage = c.CodePage;
            IDTSOutputColumn100 oc = so.OutputColumnCollection.New();
            oc.Name = c.Src;
            oc.SetDataTypeProperties(c.Type, c.Len, c.Prec, c.Scale, c.CodePage);
            oc.ExternalMetadataColumnID = ex.ID;
            oc.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
            oc.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;
            oc.ErrorOrTruncationOperation = "Conversion";
            IDTSOutputColumn100 ec = se.OutputColumnCollection.NewAt(i);
            ec.Name = c.Src;
            ec.SetDataTypeProperties(c.Type, c.Len, c.Prec, c.Scale, c.CodePage);
        }

        IDTSComponentMetaData100 rcSrc = RowCount(pipe, "RC_SOURCE", "User::RcSource");
        Connect(pipe, so, rcSrc);

        // Unicode copy of CHAR(1) for Unicode CSV files and null-safe compare
        IDTSComponentMetaData100 der = AddComp(pipe, "Derived Column", "DER_TIMELYBATCHOK_W", out w);
        Connect(pipe, FirstNonErrorOutput(rcSrc), der);
        UseCol(w, der.InputCollection[0], "TimelyBatchOK");
        IDTSOutput100 derOut = FirstNonErrorOutput(der);
        IDTSOutputColumn100 dw = w.InsertOutputColumnAt(derOut.ID, 0, "TimelyBatchOK_W", "Unicode copy of TimelyBatchOK for CSV output");
        dw.SetDataTypeProperties(RW.DataType.DT_WSTR, 1, 0, 0, 0);
        w.SetOutputColumnProperty(derOut.ID, dw.ID, "Expression", "(DT_WSTR,1)[TimelyBatchOK]");
        w.SetOutputColumnProperty(derOut.ID, dw.ID, "FriendlyExpression", "(DT_WSTR,1)[TimelyBatchOK]");
        dw.ErrorRowDisposition = DTSRowDisposition.RD_FailComponent;
        dw.TruncationRowDisposition = DTSRowDisposition.RD_FailComponent;

        // Key validation
        IDTSComponentMetaData100 csKey = AddComp(pipe, "Conditional Split", "CSPL_KEY_VALIDATION", out w);
        Connect(pipe, derOut, csKey);
        UseCol(w, csKey.InputCollection[0], "BatchNO");
        IDTSOutput100 csKeyDefault = FirstNonErrorOutput(csKey);
        IDTSOutput100 invalid = w.InsertOutput(DTSInsertPlacement.IP_BEFORE, csKeyDefault.ID);
        invalid.Name = "INVALID_KEY";
        string keyExpr = "ISNULL([BatchNO]) || TRIM([BatchNO]) == \"\"";
        w.SetOutputProperty(invalid.ID, "Expression", keyExpr);
        w.SetOutputProperty(invalid.ID, "FriendlyExpression", keyExpr);
        w.SetOutputProperty(invalid.ID, "EvaluationOrder", 0);
        csKeyDefault.Name = "VALID_KEY";

        var srcFileCols = new List<string>();
        foreach (ColDef c in cols) srcFileCols.Add(c.IsStr ? c.Src + "_W" : c.Src);

        IDTSComponentMetaData100 rcInv = RowCount(pipe, "RC_INVALID_KEY", "User::RcInvalidKey");
        Connect(pipe, invalid, rcInv);
        FlatFile(pkg, pipe, FirstNonErrorOutput(rcInv), "FF_REJECTED_INVALID_KEY", "FF_CM_REJECTED_INVALID_KEY", "Rejected_InvalidKey", srcFileCols);

        // Lookup existing rows in Oracle
        IDTSComponentMetaData100 lkp = AddComp(pipe, "Lookup", "LKP_ORACLE_EXISTING", out w);
        SetConn(lkp, cmOra);
        Connect(pipe, csKeyDefault, lkp);
        var lsql = new StringBuilder("SELECT ");
        var refXml = new StringBuilder("<referenceMetadata><referenceColumns>");
        for (int i = 0; i < cols.Count; i++)
        {
            ColDef c = cols[i];
            // CHAR(1 BYTE) is cast to NVARCHAR2 on the Oracle side so the Lookup sees DT_WSTR (verified mapping)
            string sel = c.IsStr ? "CAST(" + c.Tgt + " AS NVARCHAR2(" + c.Len + ")) AS " + c.Tgt : c.Tgt;
            RW.DataType rt = c.IsStr ? RW.DataType.DT_WSTR : c.Type;
            int rcp = c.IsStr ? 0 : c.CodePage;
            lsql.Append(sel + (i < cols.Count - 1 ? ", " : ""));
            refXml.Append("<referenceColumn name=\"" + c.Tgt + "\" dataType=\"" + rt + "\" length=\"" + c.Len +
                          "\" precision=\"" + c.Prec + "\" scale=\"" + c.Scale + "\" codePage=\"" + rcp + "\"/>");
        }
        lsql.Append(" FROM APPS.ETL_BATCH_SQL_STG WHERE BATCHNO IS NOT NULL");
        refXml.Append("</referenceColumns></referenceMetadata>");
        w.SetComponentProperty("ConnectionType", 0);
        w.SetComponentProperty("CacheType", 0);
        w.SetComponentProperty("NoMatchBehavior", 1);
        w.SetComponentProperty("SqlCommand", lsql.ToString());
        w.SetComponentProperty("ReferenceMetadataXml", refXml.ToString());
        IDTSInput100 lin = lkp.InputCollection[0];
        IDTSInputColumn100 joinCol = UseCol(w, lin, "BatchNO");
        try { w.SetInputColumnProperty(lin.ID, joinCol.ID, "JoinToReferenceColumn", "BATCHNO"); }
        catch (Exception ex) { Log.AppendLine("Join via wrapper failed (" + ex.Message + "); setting custom property directly"); joinCol.CustomPropertyCollection["JoinToReferenceColumn"].Value = "BATCHNO"; }
        IDTSOutput100 match = OutByName(lkp, "Match Output");
        IDTSOutput100 noMatch = OutByName(lkp, "No Match");
        int idx = 0;
        foreach (ColDef c in cols)
        {
            if (c.Src == "BatchNO") continue;
            IDTSOutputColumn100 oc = w.InsertOutputColumnAt(match.ID, idx++, "TGT_" + c.Tgt, "Current Oracle value");
            try { w.SetOutputColumnProperty(match.ID, oc.ID, "CopyFromReferenceColumn", c.Tgt); }
            catch (Exception ex) { Log.AppendLine("Copy via wrapper failed for " + c.Tgt + " (" + ex.Message + ")"); oc.CustomPropertyCollection["CopyFromReferenceColumn"].Value = c.Tgt; }
            if (c.IsStr) oc.SetDataTypeProperties(RW.DataType.DT_WSTR, c.Len, 0, 0, 0);
            else oc.SetDataTypeProperties(c.Type, c.Len, c.Prec, c.Scale, c.CodePage);
        }

        // New rows -> Oracle
        IDTSComponentMetaData100 rcNew = RowCount(pipe, "RC_NEW", "User::RcNew");
        Connect(pipe, noMatch, rcNew);
        IDTSComponentMetaData100 dst = AddComp(pipe, "OLE DB Destination", "OLE_DST_ORACLE_ETL_BATCH_SQL_STG", out w);
        SetConn(dst, cmOra);
        Connect(pipe, FirstNonErrorOutput(rcNew), dst);
        w.SetComponentProperty("AccessMode", 0);
        w.SetComponentProperty("OpenRowset", "\"APPS\".\"ETL_BATCH_SQL_STG\"");
        w.SetComponentProperty("DefaultCodePage", 1252);
        IDTSInput100 din = dst.InputCollection[0];
        din.ErrorRowDisposition = DTSRowDisposition.RD_RedirectRow;
        foreach (ColDef c in cols)
        {
            // OraOLEDB reports CHAR(1 BYTE) in this AL32UTF8 database as DT_WSTR -> map the Unicode copy (_W)
            IDTSExternalMetadataColumn100 ex = din.ExternalMetadataColumnCollection.New();
            ex.Name = c.Tgt;
            if (c.IsStr) { ex.DataType = RW.DataType.DT_WSTR; ex.Length = c.Len; ex.Precision = 0; ex.Scale = 0; ex.CodePage = 0; }
            else { ex.DataType = c.Type; ex.Length = c.Len; ex.Precision = c.Prec; ex.Scale = c.Scale; ex.CodePage = c.CodePage; }
            IDTSInputColumn100 ic = UseCol(w, din, c.IsStr ? c.Src + "_W" : c.Src);
            w.MapInputColumn(din.ID, ic.ID, ex.ID);
        }
        IDTSComponentMetaData100 rcRej = RowCount(pipe, "RC_REJECTED", "User::RcRejected");
        Connect(pipe, ErrorOutput(dst), rcRej);
        var errFileCols = new List<string>(srcFileCols); errFileCols.Add("ErrorCode"); errFileCols.Add("ErrorColumn");
        FlatFile(pkg, pipe, FirstNonErrorOutput(rcRej), "FF_REJECTED_INSERT_ERRORS", "FF_CM_REJECTED_INSERT_ERRORS", "Rejected_InsertErrors", errFileCols);

        // Existing rows -> change detection
        IDTSComponentMetaData100 csChg = AddComp(pipe, "Conditional Split", "CSPL_CHANGE_DETECT", out w);
        Connect(pipe, match, csChg);
        IDTSInput100 cin = csChg.InputCollection[0];
        foreach (ColDef c in cols)
        {
            if (c.Src == "BatchNO") continue;
            UseCol(w, cin, c.IsStr ? c.Src + "_W" : c.Src);
            UseCol(w, cin, "TGT_" + c.Tgt);
        }
        IDTSOutput100 csChgDefault = FirstNonErrorOutput(csChg);
        IDTSOutput100 changed = w.InsertOutput(DTSInsertPlacement.IP_BEFORE, csChgDefault.ID);
        changed.Name = "CHANGED";
        string chgExpr = ChangeExpression(cols);
        Log.AppendLine("Change expression length: " + chgExpr.Length);
        w.SetOutputProperty(changed.ID, "Expression", chgExpr);
        w.SetOutputProperty(changed.ID, "FriendlyExpression", chgExpr);
        w.SetOutputProperty(changed.ID, "EvaluationOrder", 0);
        csChgDefault.Name = "UNCHANGED";

        IDTSComponentMetaData100 rcUnch = RowCount(pipe, "RC_UNCHANGED", "User::RcUnchanged");
        Connect(pipe, csChgDefault, rcUnch);

        IDTSComponentMetaData100 rcChg = RowCount(pipe, "RC_CHANGED", "User::RcChanged");
        Connect(pipe, changed, rcChg);
        var pendCols = new List<string>(srcFileCols);
        foreach (ColDef c in cols) if (c.Src != "BatchNO") pendCols.Add("TGT_" + c.Tgt);
        FlatFile(pkg, pipe, FirstNonErrorOutput(rcChg), "FF_PENDING_CORRECTIONS", "FF_CM_PENDING_CORRECTIONS", "Pending_Corrections", pendCols);

        // ---------------- Text file logging (local dev; SSISDB logs when deployed) ----------------
        ConnectionManager logCm = pkg.Connections.Add("FILE");
        logCm.Name = "FILE_CM_ETL_TEXT_LOG";
        logCm.ConnectionString = @"E:\ETL_Logs\DBL_DIGITAL_TO_ORACLE\ETL_BATCH_SQL_STG.log";
        logCm.Properties["FileUsageType"].SetValue(logCm, DTSFileConnectionUsageType.CreateFile);
        logCm.SetExpression("ConnectionString", "@[$Package::LogFolder] + \"\\\\ETL_BATCH_SQL_STG_\" + @[User::RunStamp] + \".log\"");
        string lpCreation = null;
        foreach (LogProviderInfo li in app.LogProviderInfos) if (li.Name.Contains("Text files")) lpCreation = li.CreationName;
        if (lpCreation == null) throw new Exception("Text file log provider not found");
        LogProvider lp = pkg.LogProviders.Add(lpCreation);
        lp.Name = "LOG_TEXT_FILE";
        lp.ConfigString = logCm.Name;
        pkg.LoggingOptions.SelectedLogProviders.Add(lp);
        pkg.LoggingOptions.EventFilterKind = DTSEventFilterKind.Inclusion;
        pkg.LoggingOptions.EventFilter = new string[] { "OnError", "OnWarning", "OnTaskFailed", "OnPreExecute", "OnPostExecute", "OnInformation" };
        pkg.LoggingMode = DTSLoggingMode.Enabled;

        app.SaveToXml(outPath, pkg, null);
        Log.AppendLine("Saved: " + outPath);
        Log.AppendLine("TMP_SQL_ID=" + cmSql.ID);
        Log.AppendLine("TMP_ORA_ID=" + cmOra.ID);
    }
}
