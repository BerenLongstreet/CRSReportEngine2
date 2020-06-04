Option Strict On

Imports CrystalDecisions.CrystalReports.Engine
Imports CrystalDecisions.Shared
Imports System.Data.SqlClient
Imports System.Threading
Imports System.Reflection
Imports System.Xml
Imports System.Diagnostics
Imports System.Text
Imports System.Text.RegularExpressions
Imports System.IO
Imports System.Messaging

Public Class ReportRunner

#Region "Declarations"

    Const BASE_ERROR As Integer = vbObjectError

    Protected ErrorCount As Short = 0

    Private _ReportPath As String
    Private _ExportPath As String
    Private _CommandTimeOut As Integer
    Private _ConnectionString As String
    Private _CRSAdminConnectionString As String
    Private _CompanyDatabaseConnectionString As String

    Private _XMLReportRequest As Xml.XmlDocument

    Private ThisReport As ReportDocument
    Private ThisThreadHash As Integer = Thread.CurrentThread.GetHashCode()

    Private _ReportID As Integer

    Private ReportName, ReportFileName, ReportTitle, DataSetName, CommandName As String
    Private ExportFileName As String
    Private CompanyID, PermissionID As Integer

    Private ReportInfoConnection As New SqlConnection()
    Private CompanyDatabaseInfo As New SqlConnection()
    Private ReportInfo As New SqlConnection()

    Private ReportDataset As New DataSet()
    Private DataAdapter As New SqlDataAdapter()

    Private htReportParameters As Hashtable

    Private myRandom As New System.Random(System.Environment.TickCount)

    Protected myMutex As New Mutex(False, "CRSReportEngineReportRunnerMutex")

#End Region

#Region "Event Declarations"
    Public Event SetReportName(ByVal ThreadHash As Integer, ByVal ReportName As String, ByVal AttemptCount As Short)

    Public Event CheckingProperties(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event LoadingReportInfo(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event SettingCommandProperties(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event SettingCommandParameters(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event FillDataSetInProgress(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event FillDataSetComplete(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event LoadReportFileInProgress(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event LoadReportFileComplete(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event SettingReportParameters(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event SettingExportOptions(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event ExportInProgress(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)

    Public Event ExportComplete(ByVal ThreadHash As Integer, ByVal totalRunTime As Integer, ByVal fileName As String, ByVal ReportTitle As String, ByVal AttemptCount As Short)

    Public Event ExportError(ByVal ThreadHash As Integer, ByVal EngineError As Exception, ByVal AttemptCount As Short)

    Public Event ExportCustomError(ByVal ThreadHash As Integer, ByVal CustomErrorCode As Integer, ByVal CustomErrorSource As String, ByVal CustomErrorDescription As String, ByVal AttemptCount As Short)

    Public Event LogGenericMessage(ByVal ThreadHash As Integer, ByVal LogMessage As String, ByVal LogMessageThreshold As CRSReportEngine.MessageOutputMode)

#End Region

#Region "Property Declarations"

    Public WriteOnly Property ReportPath() As String
        Set(ByVal Value As String)
            _ReportPath = Value
        End Set
    End Property

    Public WriteOnly Property ExportPath() As String
        Set(ByVal Value As String)
            _ExportPath = Value
        End Set
    End Property

    Public WriteOnly Property AdminConnectionString() As String
        Set(ByVal Value As String)
            _CRSAdminConnectionString = Value
        End Set
    End Property

    Public WriteOnly Property CompanyDatabaseConnectionString() As String
        Set(ByVal Value As String)
            _CompanyDatabaseConnectionString = Value
        End Set
    End Property

    Public WriteOnly Property ConnectionString() As String
        Set(ByVal Value As String)
            _ConnectionString = Value
        End Set
    End Property

    Public WriteOnly Property XMLReportRequest() As XmlDocument
        Set(ByVal Value As XmlDocument)
            _XMLReportRequest = Value
        End Set
    End Property

    Public WriteOnly Property CommandTimeout() As Integer
        Set(ByVal Value As Integer)
            _CommandTimeOut = Value
        End Set
    End Property


#End Region

#Region "Event Handlers"

#End Region

#Region "Methods and Functions"
    Private Sub LogDetailedMessage(ByVal LogMessage As String)
        RaiseEvent LogGenericMessage(ThisThreadHash, LogMessage, CRSReportEngine.MessageOutputMode.WriteMessages_Detailed)
    End Sub

    Public Sub RunReport()

        Dim MethodName As String = "ReportRunner.RunReport():{0}"

        Try
            Dim startTicks As Integer = System.Environment.TickCount

            LogDetailedMessage(String.Format(MethodName, "Checking Properties"))

            Try
                myMutex.WaitOne()
                RaiseEvent CheckingProperties(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
            Catch Threadex As ThreadInterruptedException
                EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                Me.Finalize()
                Exit Sub
            Finally
                myMutex.ReleaseMutex()
            End Try
            'Make sure all required properties are set before beginning the process

            CheckProperties()

            'Get Report Info from database, plus connection string for company
            LogDetailedMessage(String.Format(MethodName, "Loading Report Info"))

            Try
                myMutex.WaitOne()
                RaiseEvent LoadingReportInfo(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
            Catch Threadex As ThreadInterruptedException
                EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                Me.Finalize()
                Exit Sub
            Finally
                myMutex.ReleaseMutex()
            End Try

            LoadReportInfo()

            LogDetailedMessage(String.Format(MethodName, "Parsing Report Request"))


            Dim ReportRequestRootNode As XmlNode = _XMLReportRequest.DocumentElement
            Dim ReportDataConnection As New SqlConnection(_ConnectionString)
            Dim ReportDataCommand As New SqlCommand()

            Try
                Try
                    myMutex.WaitOne()
                    RaiseEvent SettingCommandProperties(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
                Catch Threadex As ThreadInterruptedException
                    EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                    Me.Finalize()
                    Exit Sub
                Finally
                    myMutex.ReleaseMutex()
                End Try

                LogDetailedMessage(String.Format(MethodName, "Building Report Data Request"))

                ReportDataConnection.Open()
                With ReportDataCommand
                    .Connection = ReportDataConnection
                    .CommandText = CommandName
                    .CommandTimeout = _CommandTimeOut
                    .CommandType = CommandType.StoredProcedure

                    Try
                        myMutex.WaitOne()
                        RaiseEvent SettingCommandParameters(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
                    Catch Threadex As ThreadInterruptedException
                        EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                        Me.Finalize()
                        Exit Sub
                    Finally
                        myMutex.ReleaseMutex()
                    End Try

                    With .Parameters
                        htReportParameters = New Hashtable()
                        Dim ParameterRows() As DataRow
                        ParameterRows = ReportDataset.Tables("Parameters").Select("IsReportParameter<>1")
                        If Not ParameterRows Is Nothing Then
                            Dim ParameterRow As DataRow
                            For Each ParameterRow In ParameterRows
                                Dim ParameterID As Integer = CInt(ParameterRow.Item("ParameterID"))
                                Dim ParameterValueObject As Object = TypeCreator(ParameterRow.Item("DataType").ToString())
                                Dim ParameterName As String = ParameterRow.Item("ParameterName").ToString()

                                Dim xmlParameter As XmlNode = ReportRequestRootNode.SelectSingleNode("parameters/parameter[parameterid='" & ParameterID.ToString() & "']")
                                If Not xmlParameter Is Nothing Then
                                    ParameterValueObject = Convert.ChangeType(xmlParameter.SelectSingleNode("parametervalue").InnerText, ParameterValueObject.GetType)
                                    'Add Parameter to ReportdataCommand
                                    .AddWithValue(ParameterName, ParameterValueObject)

                                    '.AddWithValue(ParameterName, ParameterValueObject)
                                    htReportParameters.Add(ParameterName, ParameterValueObject)

                                Else
                                    'Missing a Parameter
                                    Dim IsParameterRequired As Boolean = CBool(ParameterRow.Item("Required"))
                                    If IsParameterRequired Then
                                        RaiseErrorEvent(BASE_ERROR + 1, "Adding Parameters", "Error: Parameter " & ParameterID & " is required but was not supplied.")
                                    Else
                                        .AddWithValue(ParameterName, ParameterValueObject)
                                        '.AddWithValue(ParameterName, ParameterValueObject)
                                        htReportParameters.Add(ParameterName, ParameterValueObject)

                                    End If
                                End If

                            Next
                        Else
                            'No Stored Procedure Parameters?
                        End If

                    End With

                End With

                LogDetailedMessage(String.Format(MethodName, "Executing Data Request"))

                Dim thisDataset As New DataSet()

                Dim sqlda As New SqlDataAdapter(ReportDataCommand)

                Try
                    myMutex.WaitOne()
                    RaiseEvent FillDataSetInProgress(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
                Catch Threadex As ThreadInterruptedException
                    EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                    Me.Finalize()
                    Exit Sub
                Finally
                    myMutex.ReleaseMutex()
                End Try

                sqlda.Fill(thisDataset, DataSetName)

                LogDetailedMessage(String.Format(MethodName, "Checking Result Data"))

                'Test to see if there is any data. If not there is no need to continue.
                Dim thisTable As DataTable
                Try
                    Dim NonEmptyTableCount As Integer
                    For Each thisTable In thisDataset.Tables
                        If thisTable.Rows.Count > 0 Then NonEmptyTableCount += 1
                    Next
                    If NonEmptyTableCount = 0 Then
                        'It should only come here if none of the datatables in the dataset have rows.
                        LogDetailedMessage(String.Format(MethodName, "No data was returned from the stored procedure"))

                        RaiseErrorEvent(BASE_ERROR + 22556, "Checking Dataset for data", "No data was returned from the stored procedure.")
                    End If
                Catch ex As Exception
                    Throw ex
                End Try

                LogDetailedMessage(String.Format(MethodName, "Renaming Data Tables"))

                Try
                    myMutex.WaitOne()
                    RaiseEvent FillDataSetComplete(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
                Catch Threadex As ThreadInterruptedException
                    EventLog.WriteEntry("CRSReportEngine: ", Threadex.ToString())
                    Me.Finalize()
                    Exit Sub
                Finally
                    myMutex.ReleaseMutex()
                End Try

                Dim DataSourceTableRow As DataRow
                For Each DataSourceTableRow In ReportDataset.Tables("DataTables").Select()
                    thisDataset.Tables(CInt(DataSourceTableRow.Item("OrdinalPosition"))).TableName = DataSourceTableRow.Item("DataTableName").ToString()
                Next

                LogDetailedMessage(String.Format(MethodName, "Loading Report File"))

                Try
                    myMutex.WaitOne()
                    RaiseEvent LoadReportFileInProgress(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
                Catch Threadex As ThreadInterruptedException
                    EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                    Me.Finalize()
                    Exit Sub
                Finally
                    myMutex.ReleaseMutex()
                End Try

                ThisReport = New ReportDocument()

                With ThisReport
                    Try
                        LogDetailedMessage(String.Format(MethodName, "Loading Report File - Attempt #1"))

                        .Load(_ReportPath & ReportFileName, OpenReportMethod.OpenReportByTempCopy)
                    Catch IOEx As IOException
                        'Try again one more time before fail

                        System.Threading.Thread.Sleep(2000)
                        'Thread.CurrentThread.Sleep(2000)

                        LogDetailedMessage(String.Format(MethodName, "Loading Report File - Attempt #2"))
                        .Load(_ReportPath & ReportFileName, OpenReportMethod.OpenReportByTempCopy)
                    Catch ex As Exception
                        LogDetailedMessage(String.Format(MethodName, "Problem Loading Report " & ex.Message))
                        Throw ex
                    End Try

                    If .IsLoaded Then
                        LogDetailedMessage(String.Format(MethodName, "Report Loaded"))
                    End If

                    Try
                        LogDetailedMessage(String.Format(MethodName, "Setting Data Source?"))

                        .SetDataSource(thisDataset)

                        LogDetailedMessage(String.Format(MethodName, "Data Source Set!"))
                    Catch ex As Exception
                        LogDetailedMessage(String.Format(MethodName, "Problem Setting Data Source"))
                        Throw ex
                    End Try

                End With

                Try
                    myMutex.WaitOne()
                    RaiseEvent LoadReportFileComplete(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
                Catch Threadex As ThreadInterruptedException
                    EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                    Me.Finalize()
                    Exit Sub
                Finally
                    myMutex.ReleaseMutex()
                End Try

                ''If any subreports, pass them the dataset also

                'Dim SubreportRow As DataRow

                'For Each SubreportRow In ReportDataset.Tables("SubReports").Select()

                '    Dim OrdinalPosition As Integer = CInt(SubreportRow.Item("OrdinalPosition"))
                '    Dim ThisSubReport As SubreportObject = GetSubreportObject(SubreportRow.Item("SubReportName").ToString())

                '    If Not ThisSubReport Is Nothing Then

                '    Else
                '        'Not a valid subreport - name may be wrong
                '    End If


                'Next

                ''For each subreport, see if it has parameters. Pass any parameters necessary.

                LogDetailedMessage(String.Format(MethodName, "Passing Parameters to Report"))

                Try
                    myMutex.WaitOne()
                    RaiseEvent SettingReportParameters(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
                Catch Threadex As ThreadInterruptedException
                    EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                    Me.Finalize()
                    Exit Sub
                Finally
                    myMutex.ReleaseMutex()
                End Try

                Dim crParameterFieldDefinitions As ParameterFieldDefinitions
                Dim crParameterValues As ParameterValues
                crParameterFieldDefinitions = ThisReport.DataDefinition.ParameterFields

                Dim ThisReportParameterRow As DataRow
                For Each ThisReportParameterRow In ReportDataset.Tables("Parameters").Select("IsReportParameter=1")
                    Dim ReportParameterID As Integer = CInt(ThisReportParameterRow.Item("ParameterID"))

                    Dim ReportParameterValueObject As Object = TypeCreator(ThisReportParameterRow.Item("DataType").ToString())

                    Dim ReportParameterName As String = ThisReportParameterRow.Item("ParameterName").ToString()

                    Dim XMLReportRequestParameter As XmlNode = ReportRequestRootNode.SelectSingleNode("parameters/parameter[parameterid='" & ReportParameterID & "']")
                    If Not XMLReportRequestParameter Is Nothing Then
                        ReportParameterValueObject = XMLReportRequestParameter.SelectSingleNode("parametervalue").InnerText

                        Dim crParameterFieldDefinition As ParameterFieldDefinition

                        crParameterFieldDefinition = crParameterFieldDefinitions.Item(ReportParameterName)

                        Dim crParameterDiscreteValue As New ParameterDiscreteValue

                        crParameterDiscreteValue.Value = ReportParameterValueObject

                        ' Add the parameter value
                        crParameterValues = crParameterFieldDefinition.CurrentValues
                        crParameterValues.Add(crParameterDiscreteValue)
                        htReportParameters.Add(ReportParameterName, ReportParameterValueObject)

                        ' Apply the current value to the parameter definition
                        crParameterFieldDefinition.ApplyCurrentValues(crParameterValues)

                    Else
                        'Missing Report Parameter!
                        'Is it required?
                        Dim IsParameterRequired As Boolean = CBool(ThisReportParameterRow.Item("Required"))
                        If IsParameterRequired Then
                            RaiseErrorEvent(BASE_ERROR + 1, "Adding Report Parameters", "Error: Parameter " & ReportParameterID & " is required but was not supplied.")
                        Else

                            Dim crParameterFieldDefinition As ParameterFieldDefinition
                            Dim crParameterDiscreteValue As New ParameterDiscreteValue

                            'Access first parameter field definition
                            crParameterFieldDefinition = crParameterFieldDefinitions.Item(ReportParameterName)

                            'Set discrete parameter value
                            crParameterDiscreteValue.Value = ReportParameterValueObject

                            ' Add the parameter value
                            crParameterValues = crParameterFieldDefinition.CurrentValues
                            crParameterValues.Add(crParameterDiscreteValue)
                            htReportParameters.Add(ReportParameterName, ReportParameterValueObject)

                            ' Apply the current value to the parameter definition
                            crParameterFieldDefinition.ApplyCurrentValues(crParameterValues)
                        End If

                    End If
                Next

                LogDetailedMessage(String.Format(MethodName, "Set Export Options and Report Overrides"))

                'Set Export Options
                Try
                    myMutex.WaitOne()
                    RaiseEvent SettingExportOptions(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
                Catch Threadex As ThreadInterruptedException
                    EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                    'we're done here = let the thread expire
                    Me.Finalize()
                    Exit Sub
                Finally
                    myMutex.ReleaseMutex()
                End Try

                ExportFileName = ReportName & Math.Round(myRandom.Next).ToString

                'check the report request for any output overrides
                Dim OverridesNode As XmlNode = _XMLReportRequest.DocumentElement.SelectSingleNode("overrides")
                Dim OverridePath As String = String.Empty, OriginalExportPath As String = String.Empty
                Dim OverrideFileName As String = String.Empty
                Dim OverrideReportTitle As String = String.Empty

                If Not OverridesNode Is Nothing Then
                    'Override Report Title
                    Dim OverrideReportTitleNode As XmlNode = OverridesNode.SelectSingleNode("reporttitle")
                    If Not OverrideReportTitleNode Is Nothing Then
                        OverrideReportTitle = OverrideReportTitleNode.InnerText
                        If OverrideReportTitle.IndexOf("+") <> -1 Then
                            If OverrideReportTitle.Substring(0, 2) = "++" Then
                                ReportTitle = "++" & Parameterizer(OverrideReportTitle.Substring(2))
                            Else
                                ReportTitle = Parameterizer(OverrideReportTitle)
                            End If
                        Else
                            ReportTitle = OverrideReportTitle
                        End If
                        If ReportTitle.Substring(0, 2) = "++" Then
                            ReportTitle = ThisReport.SummaryInfo.ReportTitle & ReportTitle.Substring(2)
                            ThisReport.SummaryInfo.ReportTitle = ReportTitle
                        Else
                            ThisReport.SummaryInfo.ReportTitle = ReportTitle
                        End If
                    End If

                    'Override Export Path
                    Dim OverridePathNode As XmlNode = OverridesNode.SelectSingleNode("exportpath")

                    If Not OverridePathNode Is Nothing Then
                        OverridePath = OverridePathNode.InnerText
                        If OverridePath.IndexOf("+") <> -1 Then
                            If OverridePath.StartsWith("\\") Then
                                _ExportPath = Parameterizer(OverridePath)
                            Else
                                _ExportPath &= Parameterizer(OverridePath)
                            End If

                        Else
                            If OverridePath.StartsWith("\\") Then
                                _ExportPath = OverridePath
                            Else
                                _ExportPath &= OverridePath
                            End If
                        End If

                        _ExportPath = Replace(_ExportPath, "/", "-")
                        _ExportPath = Replace(_ExportPath, ":", "_")
                        _ExportPath = Replace(_ExportPath, "*", "-")
                        _ExportPath = Replace(_ExportPath, "?", "-")
                        _ExportPath = Replace(_ExportPath, "<", "[")
                        _ExportPath = Replace(_ExportPath, ">", "]")
                        _ExportPath = Replace(_ExportPath, "|", "-")

                        If Not Directory.Exists(_ExportPath) Then
                            Try
                                Directory.CreateDirectory(_ExportPath)
                            Catch ex As Exception
                                EventLog.WriteEntry("ReportRunner", "Overriding export path for Report produced an invalid path: " & _ExportPath & Environment.NewLine() & "Default export path will be used.")
                                _ExportPath = OriginalExportPath
                            End Try
                        End If

                    End If

                    'Override Export File Name
                    Dim OverrideFileNameNode As XmlNode = OverridesNode.SelectSingleNode("exportfilename")

                    If Not OverrideFileNameNode Is Nothing Then
                        OverrideFileName = OverrideFileNameNode.InnerText
                        If OverrideFileName.IndexOf("+") <> -1 Then

                            ExportFileName = Parameterizer(OverrideFileName)

                        Else
                            ExportFileName = OverrideFileName
                        End If
                        'File names cannot contain the following characters: \ / : * ? " < > |. 
                        ExportFileName = Replace(ExportFileName, "/", "-")
                        ExportFileName = Replace(ExportFileName, "\", "-")
                        ExportFileName = Replace(ExportFileName, ":", "_")
                        ExportFileName = Replace(ExportFileName, "*", "-")
                        ExportFileName = Replace(ExportFileName, "?", "-")
                        ExportFileName = Replace(ExportFileName, "<", "[")
                        ExportFileName = Replace(ExportFileName, ">", "]")
                        ExportFileName = Replace(ExportFileName, "|", "-")
                    End If
                End If

                'set the disk file options
                Dim OutputFileName As String = _ExportPath & ExportFileName

                Dim diskOpts As New DiskFileDestinationOptions
                ThisReport.ExportOptions.ExportDestinationType =
                ExportDestinationType.DiskFile

                'set the export format and matching file extension
                Dim ExportFormatNode As XmlNode = _XMLReportRequest.DocumentElement.SelectSingleNode("exportformat")
                If Not ExportFormatNode Is Nothing Then
                    Select Case ExportFormatNode.InnerText.ToUpper()
                        Case "PDF"
                            ThisReport.ExportOptions.ExportFormatType =
                            ExportFormatType.PortableDocFormat
                            If (Not OutputFileName.EndsWith(".pdf")) OrElse OverrideFileName Is Nothing Then
                                OutputFileName &= ".pdf"
                            End If
                        Case "EXCEL"
                            ThisReport.ExportOptions.ExportFormatType =
                            ExportFormatType.Excel
                            If (Not OutputFileName.EndsWith(".xls")) OrElse OverrideFileName Is Nothing Then
                                OutputFileName &= ".xls"
                            End If
                        Case Else
                            ThisReport.ExportOptions.ExportFormatType =
                            ExportFormatType.PortableDocFormat
                            If (Not OutputFileName.EndsWith(".pdf")) OrElse OverrideFileName Is Nothing Then
                                OutputFileName &= ".pdf"
                            End If
                    End Select
                Else
                End If

                diskOpts.DiskFileName = OutputFileName

                ThisReport.ExportOptions.DestinationOptions = diskOpts

                Try
                    myMutex.WaitOne()
                    RaiseEvent ExportInProgress(Thread.CurrentThread.GetHashCode(), ErrorCount + 1S)
                Catch Threadex As ThreadInterruptedException
                    EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                    Me.Finalize()
                    Exit Sub
                Finally
                    myMutex.ReleaseMutex()
                End Try

                LogDetailedMessage(String.Format(MethodName, "Exporting"))

                ThisReport.Export()

                LogDetailedMessage(String.Format(MethodName, "Closing Report, Garbage Collection"))

                ThisReport.Close()

                If Not IsNothing(ThisReport) Then
                    ThisReport.Dispose()
                    System.GC.Collect()
                End If
                Thread.Sleep(100)

                Try
                    myMutex.WaitOne()
                    RaiseEvent ExportComplete(Thread.CurrentThread.GetHashCode(), System.Environment.TickCount - startTicks, OutputFileName, ReportTitle, ErrorCount + 1S)
                Catch Threadex As ThreadInterruptedException
                    EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                    Me.Finalize()
                    Exit Sub
                Finally
                    myMutex.ReleaseMutex()
                End Try


            Catch ex As Exception
                LogDetailedMessage(String.Format(MethodName, "Caught Error " & ex.Message))
                Throw ex
            Finally
                LogDetailedMessage(String.Format(MethodName, "Closing Data Connection"))

                ReportDataConnection.Close()

                'If Not IsNothing(ReportDataConnection) Then
                '    LogDetailedMessage(String.Format(MethodName, "Disposing Data Connection"))
                '    Try
                '        'ReportDataConnection.Dispose()
                '    Catch

                '    End Try

                'End If

            End Try

            LogDetailedMessage(String.Format(MethodName, "Bottom of method"))

        Catch LicenceEx As CrystalDecisions.CrystalReports.Engine.OutOfLicenseException
            'Crystal Reports License Error Caught. Retrying after 1500 ms pause
            LogDetailedMessage(String.Format(MethodName, LicenceEx.Message & " X " & ErrorCount.ToString()))

            System.GC.Collect()
            Thread.Sleep(1500)
            RunReport()
        Catch ReportException As Exception

            If ErrorCount >= 8S Then
                RaiseErrorEvent(ReportException)
            Else
                LogDetailedMessage(String.Format(MethodName, "Caught " & ReportException.Message & " X " & ErrorCount.ToString()))

                'Error Caught. Retrying after 500 ms pause
                Thread.Sleep(500)
                ErrorCount += 1S 'Short
                RunReport()
            End If
        Finally
            LogDetailedMessage(String.Format(MethodName, "Finalizing"))

            Me.Finalize()
        End Try
    End Sub

    Private Sub CheckProperties()

        If (_ReportPath Is Nothing) Or _
            (_ExportPath Is Nothing) Or _
            (_CRSAdminConnectionString Is Nothing) Or _
            (_ConnectionString Is Nothing) Or _
            (_XMLReportRequest Is Nothing) Or _
            (_CompanyDatabaseConnectionString Is Nothing) Or _
            (_CommandTimeOut = Nothing) Then

            RaiseErrorEvent(BASE_ERROR + 2, "PropertyCheck", "Not All Properties Set")

        End If

    End Sub

    Private Overloads Sub RaiseErrorEvent(ByVal EngineError As Exception)

        Try
            myMutex.WaitOne()
            RaiseEvent ExportError(Thread.CurrentThread.GetHashCode(), EngineError, ErrorCount + 1S)
        Catch Threadex As ThreadInterruptedException
            EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
            Thread.CurrentThread.Abort()
        Finally
            myMutex.ReleaseMutex()
        End Try

        Thread.CurrentThread.Abort()

    End Sub
    Private Overloads Sub RaiseErrorEvent(ByVal CustomErrorCode As Integer, ByVal CustomErrorSource As String, ByVal CustomErrorDescription As String)

        Try
            myMutex.WaitOne()
            RaiseEvent ExportCustomError(Thread.CurrentThread.GetHashCode(), CustomErrorCode, CustomErrorSource, CustomErrorDescription, ErrorCount + 1S)
        Catch Threadex As ThreadInterruptedException
            EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
            Thread.CurrentThread.Abort()
        Finally
            myMutex.ReleaseMutex()
        End Try

        Thread.CurrentThread.Abort()

    End Sub

    Private Sub LoadReportInfo()

        Dim ReportRequestRootNode As XmlNode = _XMLReportRequest.DocumentElement
        _ReportID = CInt(ReportRequestRootNode.SelectSingleNode("reportid").InnerText)

        ReportDataset = GetReportData(_ReportID)

        Dim ThisReportRow As DataRow = ReportDataset.Tables("Reports").Rows(0)
        Dim ServerName, DatabaseName As String
        With ThisReportRow
            ReportName = .Item("ReportName").ToString()

            Try
                myMutex.WaitOne()
                RaiseEvent SetReportName(Thread.CurrentThread.GetHashCode(), ReportName, ErrorCount + 1S)
            Catch Threadex As ThreadInterruptedException
                EventLog.WriteEntry("CRSReportEngine:", Threadex.ToString())
                Thread.CurrentThread.Abort()
            Finally
                myMutex.ReleaseMutex()
            End Try

            ReportFileName = .Item("ReportFileName").ToString()
            ReportTitle = .Item("ReportTitle").ToString()
            DataSetName = .Item("DataSetName").ToString()
            CommandName = .Item("CommandText").ToString()
            If Not IsDBNull(.Item("CommandTimeoutSeconds")) Then
                CommandTimeout = CInt(.Item("CommandTimeoutSeconds"))
            End If
            PermissionID = CInt(.Item("PermissionID"))
            ServerName = .Item("ServerName").ToString()
            DatabaseName = .Item("DatabaseName").ToString()

        End With

        Dim CompanyIDParameterNodes As XmlNodeList = ReportRequestRootNode.SelectNodes("parameters/parameter[./parametername='@CompanyID']")

        If Not CompanyIDParameterNodes Is Nothing Then

            CompanyID = CType(CompanyIDParameterNodes(0).SelectSingleNode("parametervalue").InnerText, Integer)

        Else

            RaiseErrorEvent(BASE_ERROR + 3, "LoadReportInfo", "No Company ID Parameter Found in Report Request: " & vbCrLf & _XMLReportRequest.OuterXml)

        End If

        _ConnectionString = GetCompanyDatabaseConnectionString(CompanyID, ServerName, DatabaseName)


    End Sub

    Private Function GetReportData(ByVal ReportID As Integer) As DataSet

        Dim AdminConnection As New SqlConnection(_CRSAdminConnectionString)
        Try
            AdminConnection.Open()
            Dim sqlDAReport As New SqlDataAdapter("spGetReportDetails " & ReportID.ToString(), AdminConnection)
            Dim ReportDetails As New DataSet()

            sqlDAReport.Fill(ReportDetails)

            ReportDetails.Tables(0).TableName = "Reports"
            ReportDetails.Tables(1).TableName = "DataTables"
            ReportDetails.Tables(2).TableName = "Parameters"
            'ReportDetails.Tables(3).TableName = "SubReports"
            'ReportDetails.Tables(4).TableName = "SubReportParameters"

            ReportDetails.Relations.Add(New DataRelation("Reports_DataTables", ReportDetails.Tables("Reports").Columns("ReportID"), ReportDetails.Tables("DataTables").Columns("ReportID")))
            ReportDetails.Relations.Add(New DataRelation("Reports_Parameters", ReportDetails.Tables("Reports").Columns("ReportID"), ReportDetails.Tables("Parameters").Columns("ReportID")))
            'ReportDetails.Relations.Add(New DataRelation("Reports_SubReports", ReportDetails.Tables("Reports").Columns("ReportID"), ReportDetails.Tables("SubReports").Columns("ReportID")))
            'ReportDetails.Relations.Add(New DataRelation("SubReports_Parameters", ReportDetails.Tables("SubReports").Columns("SubReportID"), ReportDetails.Tables("SubReportParameters").Columns("SubReportID")))


            Return ReportDetails

        Catch ex As Exception
            RaiseErrorEvent(ex)
            Return Nothing
        Finally
            AdminConnection.Close()
        End Try

    End Function

    Private Function GetCompanyDatabaseConnectionString(ByVal intCompanyID As Integer, ByVal overrideServerName As String, ByVal overrideDatabase As String) As String
        Const ServerNamePrefix As String = "Data Source="
        Const DatabaseNamePrefix As String = "Initial Catalog="

        Dim FinalConnectionString As New System.Text.StringBuilder()
        FinalConnectionString.Append(_ConnectionString)

        If Not (overrideServerName.Length = 0 And overrideDatabase.Length = 0) Then

            With FinalConnectionString
                .Append(ServerNamePrefix)
                .Append(overrideServerName)
                .Append(";")

                .Append(DatabaseNamePrefix)
                .Append(overrideDatabase)
                .Append(";")
            End With

            Return FinalConnectionString.ToString()

        End If

        Dim CompanyDatabaseConnection As New SqlConnection(_CompanyDatabaseConnectionString)

        Try
            CompanyDatabaseConnection.Open()
            Dim sqlcmd As New SqlCommand("SELECT DBName, DBServer, DBDatabase, CompanyID FROM Databases WHERE CompanyID=@CompanyID")
            sqlcmd.Connection = CompanyDatabaseConnection
            sqlcmd.Parameters.AddWithValue("@CompanyID", intCompanyID)
            'sqlcmd.Parameters.AddWithValue("@CompanyID", intCompanyID)

            Dim sqldrReportDatabase As SqlDataReader = sqlcmd.ExecuteReader

            Do While sqldrReportDatabase.Read()
                With FinalConnectionString
                    .Append(ServerNamePrefix)
                    If IsDBNull(overrideServerName) Or overrideServerName.ToString = "" Then
                        .Append(sqldrReportDatabase.Item("DBServer").ToString())
                    Else
                        .Append(overrideServerName)
                    End If
                    .Append(";")

                    .Append(DatabaseNamePrefix)

                    If IsDBNull(overrideDatabase) Or overrideDatabase.ToString = "" Then
                        .Append(sqldrReportDatabase.Item("DBDatabase").ToString())
                    Else
                        .Append(overrideDatabase)
                    End If

                    .Append(";")
                End With
                Exit Do
            Loop

            sqldrReportDatabase.Close()

            Return FinalConnectionString.ToString()

        Catch ex As Exception
            RaiseErrorEvent(ex)
            Return String.Empty
        Finally
            CompanyDatabaseConnection.Close()

        End Try

    End Function

    Private Function GetSubreportObject(ByVal ReportObjectName As String) As SubreportObject

        Dim SubReport As SubreportObject

        ' Get the ReportObject by name, cast it as a SubreportObject, and return it.

        If TypeOf (ThisReport.ReportDefinition.ReportObjects.Item(ReportObjectName)) Is SubreportObject Then
            SubReport = CType(ThisReport.ReportDefinition.ReportObjects.Item(ReportObjectName), SubreportObject)
            GetSubreportObject = SubReport
        Else
            GetSubreportObject = Nothing
        End If

    End Function

    Private Function TypeCreator(ByVal TypeName As String) As Object

        Select Case LCase(TypeName)
            Case "system.string"
                Dim newObj As String = ""
                Return newObj
            Case "system.int32", "system.integer"
                Dim newObj As Integer = 0I
                Return newObj
            Case "system.int16", "system.short"
                Dim newObj As Short = 0S
                Return newObj
            Case "system.int64", "system.long"
                Dim newObj As Long = 0L
                Return newObj
            Case "system.boolean"
                Dim newObj As Boolean = False
                Return newObj
            Case "system.date", "system.datetime"
                Dim newObj As New Date()
                Return newObj
            Case "system.single"
                Dim newObj As Single = 0.0F
                Return newObj
            Case "system.double"
                Dim newObj As Double = 0.0R
                Return newObj
            Case "system.object"
                Dim newObj As New Object()
                Return newObj
            Case "system.decimal"
                Dim newObj As Decimal = 0D
                Return newObj
            Case "system.byte"
                Dim newObj As Byte = 0
                Return newObj
            Case "system.char"
                Dim newObj As New Char()
                Return newObj
            Case Else
                Return New Object()
        End Select

    End Function

    Private Function Parameterizer(ByVal StringWithParameters As String) As String
        Try

            If Not StringWithParameters.Length = 0 AndAlso StringWithParameters.IndexOf("+"c) > -1 Then


                Dim ParameterizedStringBuilder As New StringBuilder()

                Dim ParameterizedStringTokenArray As String() = StringWithParameters.Split(New Char() {"+"c})

                Dim ThisToken As String

                For Each ThisToken In ParameterizedStringTokenArray
                    If ThisToken.Trim.IndexOf("@") = 0 Then
                        Try
                            Dim ThisParameterValue As Object = ReturnParameterValue(ThisToken.Trim)

                            If IsDate(ThisParameterValue) Then
                                ParameterizedStringBuilder.Append(Format(CType(ThisParameterValue, Date), "yyyyMMdd"))
                            Else
                                ParameterizedStringBuilder.Append(ThisParameterValue.ToString())
                            End If

                        Catch EX As Exception
                            ParameterizedStringBuilder.Append(ThisToken)
                        End Try

                    ElseIf ThisToken.Trim.IndexOf("{") = 0 AndAlso ThisToken.Trim.IndexOf("}") = ThisToken.Trim.Length - 1 Then
                        Try
                            ParameterizedStringBuilder.Append(EvaluateTokenFunction(ThisToken.Trim))
                        Catch ex As Exception
                            ParameterizedStringBuilder.Append(ThisToken)
                        End Try
                    Else
                        ParameterizedStringBuilder.Append(ThisToken)
                    End If
                Next

                Return ParameterizedStringBuilder.ToString()

            Else
                Return StringWithParameters
            End If
        Catch ex As Exception
            Return StringWithParameters
        End Try

    End Function

    Private Function EvaluateTokenFunction(ByVal TokenToEvaluate As String) As String
        Dim FunctionName As String = String.Empty
        Dim FunctionArguments As ArrayList = New ArrayList()
        Try

            Try
                If TokenToEvaluate.Trim.IndexOf("{") = 0 AndAlso TokenToEvaluate.Trim.IndexOf("}") = TokenToEvaluate.Trim.Length - 1 Then
                    If TokenToEvaluate.Trim.IndexOf("(") > 0 AndAlso TokenToEvaluate.Trim.IndexOf(")") > 0 Then
                        TokenToEvaluate = TokenToEvaluate.Replace("{"c, "").Replace("}"c, "")
                        FunctionName = TokenToEvaluate.Split("("c)(0)


                        FunctionArguments = SplitQuoted(TokenToEvaluate.Split("("c)(1).Replace(")"c, ""))
                        Dim FunctionArgumentsCopy() As Object = FunctionArguments.ToArray()

                        'go through all the function arguments and see if they are parameter values

                        Dim ThisArgument As String = String.Empty
                        For Each ThisArgument In FunctionArgumentsCopy
                            If ThisArgument.StartsWith("@") Then
                                FunctionArguments(FunctionArguments.IndexOf(ThisArgument)) = ReturnParameterValue(ThisArgument)
                            End If
                        Next
                    Else
                        'No arguments for the function
                    End If

                    Select Case FunctionName.ToLower
                        Case "format"
                            If FunctionArguments.Count = 1 Then
                                Return Format(FunctionArguments(0))
                            ElseIf FunctionArguments.Count >= 2 Then
                                Return Format(FunctionArguments(0), FunctionArguments(1).ToString().Replace(Chr(34), ""))
                            Else
                                'syntax error
                                Throw New ApplicationException("Wrong number of arguments for the Format function.")
                            End If
                        Case "right"
                            If FunctionArguments.Count = 2 Then
                                Return Right(FunctionArguments(0).ToString(), CInt(FunctionArguments(1)))
                            Else
                                'syntax error
                                Throw New ApplicationException("Wrong number of arguments for the Right function.")
                            End If
                        Case "left"
                            If FunctionArguments.Count = 2 Then
                                Return Left(FunctionArguments(0).ToString(), CInt(FunctionArguments(1)))
                            Else
                                'syntax error
                                Throw New ApplicationException("Wrong number of arguments for the Left function.")
                            End If
                        Case "mid"
                            If FunctionArguments.Count = 3 Then
                                Return Mid(FunctionArguments(0).ToString(), CInt(FunctionArguments(1)), CInt(FunctionArguments(2)))
                            Else
                                'syntax error
                                Throw New ApplicationException("Wrong number of arguments for the Mid function.")
                            End If
                        Case Else
                            'this function is not supported
                            Throw New ApplicationException("Unsupported function.")
                    End Select
                Else
                    'Raise bad syntax error
                    Throw New ApplicationException("Syntax error: parenthases expected.")

                End If

            Catch ex As Exception
                Throw ex
            End Try

        Catch evalTokenFunctionException As Exception
            Throw evalTokenFunctionException
        End Try

    End Function

    Private Function ReturnParameterValue(ByVal ParameterName As String) As Object
        Dim ThisParameterValue As New Object()
        Try
            ThisParameterValue = htReportParameters.Item(ParameterName)
        Catch paramEx As Exception
            ThisParameterValue = ThisParameterValue
        End Try

        Return ThisParameterValue
    End Function


    ' split a string, dealing correctly with quoted items
    '
    ' TEXT is the string to be split
    ' SEPARATOR is the separator char (default is comma)
    ' QUOTES is the character used to quote strings (default is """",
    '  the double quote)
    '    you can also use a character pair (eg "{}") if the opening
    '    and closing quotes are different
    '
    ' for example you can split the following string
    '     arr() = SplitQuoted("[one,two],three,[four,five]", , "[]")
    ' into 3 items, because commas inside []
    ' are not taken into account

    Function SplitQuoted(ByVal Text As String, Optional ByVal Separator As String = _
        ",", Optional ByVal Quotes As String = """") As ArrayList
        ' this is the result 
        Dim res As New ArrayList()
        ' get the open and close chars, escape them for using in regular expressions
        Dim openChar As String = System.Text.RegularExpressions.Regex.Escape _
            (Quotes.Chars(0))
        Dim closeChar As String = System.Text.RegularExpressions.Regex.Escape _
            (Quotes.Chars(Quotes.Length - 1))
        ' build the patter that searches for both quoted and unquoted elements
        ' notice that the quoted element is defined by group #2 
        ' and the unquoted element is defined by group #3
        Dim pattern As String = "\s*(" & openChar & "([^" & closeChar & "]*)" & _
            closeChar & "|([^" & Separator & "]+))\s*"

        ' search all the elements
        Dim m As System.Text.RegularExpressions.Match
        For Each m In System.Text.RegularExpressions.Regex.Matches(Text, pattern)
            ' get a reference to the unquoted element, if it's there
            Dim g3 As String = m.Groups(3).Value
            If Not (g3 Is Nothing) AndAlso g3.Length > 0 Then
                ' if the 3rd group is not null, then the element wasn't quoted
                res.Add(g3)
            Else
                ' get the quoted string, but without the quotes
                res.Add(m.Groups(2).Value)
            End If
        Next
        Return res
    End Function

#End Region

End Class