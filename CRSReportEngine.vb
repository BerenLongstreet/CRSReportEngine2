Option Strict On
Option Explicit On 

Imports System.ServiceProcess

Imports System.Xml
Imports CrystalDecisions.Shared
Imports CrystalDecisions.CrystalReports.Engine
Imports System.Threading
Imports System.Messaging
Imports System.Diagnostics
Imports System.Reflection
Imports System.Timers
Imports Microsoft.Win32

Public Class CRSReportEngine
    Inherits System.ServiceProcess.ServiceBase

#Region "Declarations"

    Friend Enum ReportPriorityList As Short
        Normal = 3
        High = 5
    End Enum

    Public Enum MessageOutputMode As Byte
        WriteMessages_Off = 0
        WriteMessages_Receive_OR_Error = 1
        WriteMessages_Detailed = 2
        WriteMessage_Everything = 3
    End Enum

    Protected AppName As String

    Protected HeartBeat As Integer = 0
    Protected MessageMode As MessageOutputMode = MessageOutputMode.WriteMessages_Detailed

    Protected ReportPath, ExportPath As String
    Protected QueueLocationMachineName As String
    Protected ConnectionString, CRSAdminConnectionString, CompanyDatabaseConnectionString As String
    Protected CommandTimeOut As Integer

    Protected WorkerList As New WorkerList()

    Protected WithEvents MainQueue As MessageQueue
    Protected WithEvents ImmediateQueue As MessageQueue
    Protected WithEvents ReturnQueue As MessageQueue
    Protected WithEvents ControlQueueIn As New MessageQueue()
    Protected WithEvents ControlQueueOut As New MessageQueue()

    Protected MainFormatter, ImmediateFormatter, ReturnFormatter, ControlInFormatter, ControlOutFormatter As XmlMessageFormatter

    Protected WithEvents ThisTimer As New System.Timers.Timer(100)
    Protected WithEvents RptRunner As ReportRunner

    'Protected RunningReportCounter As PerformanceCounter

#End Region

#Region " Component Designer generated code "

    Public Sub New()
        MyBase.New()
        Try

            ' This call is required by the Component Designer.
            InitializeComponent()

            ' Add any initialization after the InitializeComponent() call
            AppName = Me.ServiceName
            'Thread.CurrentThread.Priority = ThreadPriority.AboveNormal
        Catch ex As Exception
            EventLog.WriteEntry("Error Initializing " & Me.ServiceName & ": " & ex.ToString())
            Throw ex
        End Try
        Try

            If Not EventLog.SourceExists("CRSReportEngineLog") Then
                EventLog.CreateEventSource("CRSReportEngineLog", "EngineEventLog")
            End If

            CRSReportEngineEventLog.Source = "CRSReportEngineLog"
            CRSReportEngineEventLog.Log = "EngineEventLog"

        Catch ex As Exception
            EventLog.WriteEntry("Error Creating Custom Event Log 'CRSReportEngineLog': " & ex.ToString())
            Throw ex
        End Try

    End Sub

    'UserService overrides dispose to clean up the component list.
    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

    ' The main entry point for the process
    <MTAThread()>
    Shared Sub Main()
        Dim ServicesToRun() As System.ServiceProcess.ServiceBase

        ' More than one NT Service may run within the same process. To add
        ' another service to this process, change the following line to
        ' create a second service object. For example,
        '
        '   ServicesToRun = New System.ServiceProcess.ServiceBase () {New Service1, New MySecondUserService}
        '
        ServicesToRun = New System.ServiceProcess.ServiceBase() {New CRSReportEngine()}

        System.ServiceProcess.ServiceBase.Run(ServicesToRun)
    End Sub

    'Required by the Component Designer
    Private components As System.ComponentModel.IContainer

    ' NOTE: The following procedure is required by the Component Designer
    ' It can be modified using the Component Designer.  
    ' Do not modify it using the code editor.
    Friend WithEvents SqlConnection1 As System.Data.SqlClient.SqlConnection
    Friend WithEvents CRSReportEngineEventLog As System.Diagnostics.EventLog
    Friend WithEvents CRSReportEngineControlConsoleOut As System.Messaging.MessageQueue
    Friend WithEvents CRSReportEngineControlConsoleIn As System.Messaging.MessageQueue
    <System.Diagnostics.DebuggerStepThrough()> Private Sub InitializeComponent()
        Me.SqlConnection1 = New System.Data.SqlClient.SqlConnection()
        Me.CRSReportEngineEventLog = New System.Diagnostics.EventLog()
        Me.CRSReportEngineControlConsoleOut = New System.Messaging.MessageQueue()
        Me.CRSReportEngineControlConsoleIn = New System.Messaging.MessageQueue()
        CType(Me.CRSReportEngineEventLog, System.ComponentModel.ISupportInitialize).BeginInit()
        '
        'SqlConnection1
        '
        Me.SqlConnection1.ConnectionString = "data source=CRSAPP;initial catalog=CRSAdmin;password=opsdb;persist security info" &
        "=True;user id=OpsDB;workstation id=CRSUSR05;packet size=4096"
        '
        'CRSReportEngineControlConsoleOut
        '
        Me.CRSReportEngineControlConsoleOut.Formatter = New System.Messaging.XmlMessageFormatter(New String(-1) {})
        Me.CRSReportEngineControlConsoleOut.Path = ".\CRSReportEngineControlConsoleOut"
        '
        'CRSReportEngineControlConsoleIn
        '
        Me.CRSReportEngineControlConsoleIn.Formatter = New System.Messaging.XmlMessageFormatter(New String(-1) {})
        Me.CRSReportEngineControlConsoleIn.Path = ".\CRSReportEngineControlConsoleIn"
        '
        'CRSReportEngine
        '
        Me.CanPauseAndContinue = True
        Me.CanShutdown = True
        Me.ServiceName = "CRSReportEngine"
        CType(Me.CRSReportEngineEventLog, System.ComponentModel.ISupportInitialize).EndInit()

    End Sub

#End Region

#Region "Event Handlers"

    Protected Overrides Sub OnStart(ByVal StartupArguments() As String)

#If DEBUG Then
        Thread.Sleep(20000)
#End If

        ' Add code here to start your service. This method should set things
        ' in motion so your service can do its work.
        Try
            CRSReportEngineEventLog.WriteEntry("Service Started")
        Catch ex As Exception
            Throw ex
        End Try
        Try
            ' Retrieve Settings
            RetrieveSettings()
            If MessageMode > MessageOutputMode.WriteMessages_Off Then
                CRSReportEngineEventLog.WriteEntry("Settings Retrieved")
            End If
            '   
            ' Initialize Queue Listeners
            If MessageMode > MessageOutputMode.WriteMessages_Off Then
                CRSReportEngineEventLog.WriteEntry("Initializing Message Queues")
            End If
            InitializeQueueListeners()
            If MessageMode > MessageOutputMode.WriteMessages_Off Then
                CRSReportEngineEventLog.WriteEntry("Message Queues Initialized Successfully")
            End If

            'If Not PerformanceCounterCategory.Exists(AppName) Then
            '    PerformanceCounterCategory.Create(AppName, "Performance counters for " & AppName, "Running Reports Count", "Number of currently running reports on this machine.")
            'End If
            'RunningReportCounter = New PerformanceCounter(AppName, "Running Reports Count", False)

            If MessageMode > MessageOutputMode.WriteMessages_Off Then
                CRSReportEngineEventLog.WriteEntry("Performance Counter Initialized Successfully")
            End If


            ' Start Timer
            ThisTimer.Interval = HeartBeat
            ThisTimer.Start()

        Catch serviceEx As Exception
            'Log Error
            CRSReportEngineEventLog.WriteEntry("Problem during start service command:" & vbCrLf & serviceEx.ToString())
        Finally
        End Try
    End Sub

    Protected Overrides Sub OnStop()
        ' Add code here to perform any tear-down necessary to stop your service.
        ' Kill Console Queues on Local Machine
        Try
            ThisTimer.Stop()
            KillPrivateQueues()

            If MessageMode > MessageOutputMode.WriteMessages_Off Then
                CRSReportEngineEventLog.WriteEntry("Service Stopping...")
            End If

            Dim ThreadHashKeyCode As Integer, TornThreadCount As Integer
            For Each ThreadHashKeyCode In WorkerList.WorkerList.Keys
                Dim ThisThread As Thread = WorkerList.GetWorkerListReportThread(ThreadHashKeyCode)
                ThisThread.Abort()
                TornThreadCount += 1
            Next

            If MessageMode > MessageOutputMode.WriteMessages_Off Then
                CRSReportEngineEventLog.WriteEntry(TornThreadCount & " Threads Torn Down Successfully...")
            End If

            'RunningReportCounter.Dispose()
            'If PerformanceCounterCategory.Exists(AppName) Then
            '    PerformanceCounterCategory.Delete(AppName)
            'End If

            If MessageMode > MessageOutputMode.WriteMessages_Off Then
                CRSReportEngineEventLog.WriteEntry("Performance Counter Removed Successfully...")
                CRSReportEngineEventLog.WriteEntry("Service Stopping...")
            End If


        Catch ex As Exception

            CRSReportEngineEventLog.WriteEntry("Problem during stop service command:" & vbCrLf & ex.ToString())

        End Try

    End Sub

    Private Sub thisTimer_Elapsed(ByVal sender As System.Object, ByVal e As System.Timers.ElapsedEventArgs) Handles ThisTimer.Elapsed
        If Me.MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("Heartbeat...")
        End If

        If WorkerList.TotalWorkingThreads < 5 Then
            PeekAtReportQueues()
        Else
            'CRSReportEngineEventLog.WriteEntry("Waiting on instance to finish " & TotalWorkingThreads)
        End If
    End Sub

    Protected Overrides Sub OnPause()
        If MessageMode > MessageOutputMode.WriteMessages_Off Then
            CRSReportEngineEventLog.WriteEntry("Service Paused")
        End If

        ThisTimer.Stop()
    End Sub

    Protected Overrides Sub OnContinue()
        If MessageMode >= MessageOutputMode.WriteMessages_Receive_OR_Error Then
            CRSReportEngineEventLog.WriteEntry("Service Resumed")
        End If

        ThisTimer.Start()
    End Sub

    Private Sub RptRunner_ExportComplete(ByVal ThreadHash As Integer, ByVal totalRunTime As Integer, ByVal fileName As String, ByVal ReportTitle As String, ByVal AttemptCount As Short)
        If MessageMode >= MessageOutputMode.WriteMessages_Receive_OR_Error Then
            CRSReportEngineEventLog.WriteEntry("Export Complete Notification from Thread " & ThreadHash.ToString())
        End If

        Try

            WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
            WorkerList.ChangeStatusText(ThreadHash, "Export Complete")

            Dim ReturnReportRequest As ReportInfo = WorkerList.GetWorkerListReportInfo(ThreadHash)

            WorkerList.RemoveWorkerBee(ThreadHash, "RptRunner_ExportComplete")
            'RunningReportCounter.Decrement()

            Dim ReturnMessageString As New System.Text.StringBuilder
            With ReturnMessageString

                .Append("<returnmessage>" & vbCrLf)
                .Append("<status>Complete</status>" & vbCrLf)

                .Append("<requestmessageid>")
                .Append(ReturnReportRequest.MessageID)
                .Append("</requestmessageid>" & vbCrLf)

                .Append("<requestpriority>")
                .Append(ReportInfo.ReportPriority.GetName(GetType(ReportInfo.ReportPriority), ReturnReportRequest.Priority))
                .Append("</requestpriority>" & vbCrLf)

                .Append("<outputfilename>")
                .Append(IIf(IsNothing(fileName) Or fileName.Length = 0, "", fileName))
                .Append("</outputfilename>")

                .Append("<reporttitle>")
                .Append(ReportTitle)
                .Append("</reporttitle>")

                .Append("<processtime>")
                .Append(totalRunTime)
                .Append("</processtime>")

                .Append("<originalrequest><![CDATA[")
                .Append(ReturnReportRequest.ReportRequestXML.OuterXml)
                .Append("]]></originalrequest>")

                .Append("<attemptcount>")
                .Append(ReturnReportRequest.AttemptCount)
                .Append("</attemptcount>")

                .Append("</returnmessage>")

            End With

            Dim ReturnMessage As New Message(ReturnMessageString.ToString(), New XmlMessageFormatter)

            ReturnMessage.CorrelationId = ReturnReportRequest.MessageID
            ReturnMessage.Label = ReturnReportRequest.MessageLabel

            ReturnQueue.Send(ReturnMessage)
        Catch ex As Exception

            If MessageMode >= MessageOutputMode.WriteMessages_Receive_OR_Error Then
                CRSReportEngineEventLog.WriteEntry("Export Complete Notification Error:" & vbCrLf & ex.ToString())
            End If

        End Try

    End Sub

    Private Sub RptRunner_ExportError(ByVal ThreadHash As Integer, ByVal EngineEx As Exception, ByVal AttemptCount As Short)
        If MessageMode >= MessageOutputMode.WriteMessages_Receive_OR_Error Then
            CRSReportEngineEventLog.WriteEntry("Export Error Notification from Thread " & ThreadHash.ToString())
        End If
        Try
            Debug.WriteLine("Export Error on Thread #" & ThreadHash.ToString())

            WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
            WorkerList.ChangeStatusText(ThreadHash, "Error:" & EngineEx.ToString())

            Dim ReturnReportRequest As ReportInfo = WorkerList.GetWorkerListReportInfo(ThreadHash)
            If IsNothing(ReturnReportRequest) Then
                Throw New Exception("Thread #" & ThreadHash.ToString() & " may have been aborted.")
            End If

            WorkerList.RemoveWorkerBee(ThreadHash, "RptRunner_ExportError")
            'RunningReportCounter.Decrement()

            Dim ReturnMessageString As New System.Text.StringBuilder
            With ReturnMessageString

                .Append("<returnmessage>" & vbCrLf)
                .Append("<status>Error</status>" & vbCrLf)

                .Append("<requestmessageid>")
                .Append(ReturnReportRequest.MessageID)
                .Append("</requestmessageid>" & vbCrLf)

                .Append("<requestpriority>")
                .Append(ReportInfo.ReportPriority.GetName(GetType(ReportInfo.ReportPriority), ReturnReportRequest.Priority))
                .Append("</requestpriority>" & vbCrLf)

                .Append("<errordetails><![CDATA[")
                .Append(EngineEx.ToString())
                .Append("]]></errordetails>")

                .Append("<originalrequest><![CDATA[")
                .Append(ReturnReportRequest.ReportRequestXML.OuterXml)
                .Append("]]></originalrequest>")

                .Append("<attemptcount>")
                .Append(ReturnReportRequest.AttemptCount)
                .Append("</attemptcount>")

                .Append("</returnmessage>")

            End With

            If MessageMode >= MessageOutputMode.WriteMessages_Receive_OR_Error Then
                CRSReportEngineEventLog.WriteEntry(ReturnMessageString.ToString())
            End If

            CRSReportEngineEventLog.WriteEntry(ReturnMessageString.ToString())

            Dim ReturnMessage As New Message(ReturnMessageString.ToString(), New XmlMessageFormatter)

            ReturnMessage.CorrelationId = ReturnReportRequest.MessageID
            ReturnMessage.Label = ReturnReportRequest.MessageLabel

            ReturnQueue.Send(ReturnMessage)


        Catch ex As Exception
            CRSReportEngineEventLog.WriteEntry("Error Returning 'Export Error Notification':" & vbCrLf & ex.ToString())
            Debug.WriteLine("Thread #" & ThreadHash.ToString & " - Error Returning 'Export Error Notification':" & vbCrLf & ex.ToString())

        End Try

    End Sub

    Private Sub RptRunner_ExportCustomError(ByVal ThreadHash As Integer, ByVal CustomErrorCode As Integer, ByVal CustomErrorSource As String, ByVal CustomErrorDescription As String, ByVal AttemptCount As Short)
        If MessageMode >= MessageOutputMode.WriteMessages_Receive_OR_Error Then
            CRSReportEngineEventLog.WriteEntry("Export Custom Error Notification from Thread " & ThreadHash.ToString())
        End If

        Try
            Dim ErrorString As String = "Error:" & CustomErrorCode.ToString() & ControlChars.CrLf & " Source=" & CustomErrorSource & ControlChars.CrLf & " - Description=" & CustomErrorDescription

            WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
            WorkerList.ChangeStatusText(ThreadHash, ErrorString)

            Dim ReturnReportRequest As ReportInfo = WorkerList.GetWorkerListReportInfo(ThreadHash)

            WorkerList.RemoveWorkerBee(ThreadHash, "RptRunner_ExportCustomError")
            'RunningReportCounter.Decrement()

            Dim ReturnMessageString As New System.Text.StringBuilder
            With ReturnMessageString

                .Append("<returnmessage>" & vbCrLf)
                .Append("<status>Error</status>" & vbCrLf)

                .Append("<requestmessageid>")
                .Append(ReturnReportRequest.MessageID)
                .Append("</requestmessageid>" & vbCrLf)

                .Append("<requestpriority>")
                .Append(ReportInfo.ReportPriority.GetName(GetType(ReportInfo.ReportPriority), ReturnReportRequest.Priority))
                .Append("</requestpriority>" & vbCrLf)

                .Append("<errordetails><![CDATA[")
                .Append(ErrorString)
                .Append("]]></errordetails>")

                .Append("<originalrequest><![CDATA[")
                .Append(ReturnReportRequest.ReportRequestXML.OuterXml)
                .Append("]]></originalrequest>")

                .Append("<attemptcount>")
                .Append(ReturnReportRequest.AttemptCount)
                .Append("</attemptcount>")

                .Append("</returnmessage>")

            End With

            If MessageMode >= MessageOutputMode.WriteMessages_Receive_OR_Error Then
                CRSReportEngineEventLog.WriteEntry(ReturnMessageString.ToString())
            End If

            Dim ReturnMessage As New Message(ReturnMessageString.ToString(), New XmlMessageFormatter)

            ReturnMessage.CorrelationId = ReturnReportRequest.MessageID
            ReturnMessage.Label = ReturnReportRequest.MessageLabel

            ReturnQueue.Send(ReturnMessage)

        Catch ex As Exception

            CRSReportEngineEventLog.WriteEntry("Error Returning 'Export Error Notification':" & vbCrLf & ex.ToString())
            Debug.WriteLine("Thread #" & ThreadHash.ToString & " - Error Returning 'Export Error Notification':" & vbCrLf & ex.ToString())

        End Try

    End Sub

    Private Sub LogGenericMessage(ByVal ThreadHash As Integer, ByVal LogMessage As String, ByVal LogMessageThreshold As CRSReportEngine.MessageOutputMode)
        If MessageMode >= LogMessageThreshold Then
            CRSReportEngineEventLog.WriteEntry(String.Format("Thread #{0}:{1}", ThreadHash, LogMessage))
        End If
    End Sub

    Private Sub CheckingProperties(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("CheckingProperties Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Checking Properties")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("CheckingProperties Finish from Thread " & ThreadHash.ToString())
        End If

    End Sub

    Private Sub LoadingReportInfo(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("LoadingReportInfo Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Loading Report Info")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("LoadingReportInfo Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

    Private Sub SettingCommandProperties(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("SettingCommandProperties Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Setting Command Properties")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("SettingCommandProperties Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

    Private Sub SettingCommandParameters(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("SettingCommandParameters Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Setting Command Parameters")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("SettingCommandParameters Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

    Private Sub FillDataSetInProgress(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("FillDataSetInProgress Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Fill DataSet In Progress")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("FillDataSetInProgress Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

    Private Sub FillDataSetComplete(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("FillDataSetComplete Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Fill DataSet Complete")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("FillDataSetComplete Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

    Private Sub LoadReportFileInProgress(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("LoadReportFileInProgress Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Load Report File In Progress")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("LoadReportFileInProgress Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

    Private Sub LoadReportFileComplete(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("LoadReportFileComplete Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Load Report File Complete")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("LoadReportFileComplete Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

    Private Sub SettingReportParameters(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("SettingReportParameters Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Setting Report Parameters")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("SettingReportParameters Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

    Private Sub SettingExportOptions(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("SettingExportOptions Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Setting Export Options")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("SettingExportOptions Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

    Private Sub ExportInProgress(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("ExportInProgress Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeStatusText(ThreadHash, "Export In Progress")

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("ExportInProgress Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

    Private Sub ReportNameSet(ByVal ThreadHash As Integer, ByVal newReportName As String, ByVal AttemptCount As Short)
        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("ReportNameSet Start from Thread " & ThreadHash.ToString())
        End If

        WorkerList.ChangeAttemptCount(ThreadHash, AttemptCount)
        WorkerList.ChangeReportName(ThreadHash, newReportName)

        If MessageMode = MessageOutputMode.WriteMessage_Everything Then
            CRSReportEngineEventLog.WriteEntry("ReportNameSet Finish from Thread " & ThreadHash.ToString())
        End If
    End Sub

#End Region

#Region "Methods and Functions"

    Protected Sub RetrieveSettings()
        Try

            '   Heartbeat
            HeartBeat = CInt(GetSoftwareSetting("HeartbeatMilliseconds", "100"))
            '   Report Path
            ReportPath = GetSoftwareSetting("ReportPath", Environment.CurrentDirectory)
            '   Export Path
            ExportPath = GetSoftwareSetting("ExportPath", Environment.CurrentDirectory)
            '   Queue Location Machine Name
            QueueLocationMachineName = GetSoftwareSetting("QueueLocationMachineName", ".")
            '   CRSAdmin Database Connection String
            CRSAdminConnectionString = GetSoftwareSetting("CRSReportsConnectionString", "")
            '   Company Database Connection String
            CompanyDatabaseConnectionString = GetSoftwareSetting("CompanyDatabaseConnectionString", "")
            '   Database Connection String (without server or database name)
            ConnectionString = GetSoftwareSetting("ConnectionString", "")
            '   Command Timeout in seconds
            CommandTimeOut = CInt(GetSoftwareSetting("CommandTimeout", "90"))
            '   Message Output Mode
            MessageMode = CType(GetSoftwareSetting("MessageOutputMode", "2"), MessageOutputMode)

        Catch ex As Exception
            CRSReportEngineEventLog.WriteEntry("Problem retrieving settings from registry:" & vbCrLf & ex.ToString())
            End
        End Try

    End Sub

    Protected Function GetSoftwareSetting(ByVal KeyName As String, ByVal DefaultValue As String) As String
        Dim tempValueString As String = ""

        Dim SoftwareKey As RegistryKey = Registry.LocalMachine.OpenSubKey("SOFTWARE")
        Dim CRSCompanyKey As RegistryKey = SoftwareKey.OpenSubKey("CRS")
        Dim thisApplication As RegistryKey = CRSCompanyKey.OpenSubKey(AppName)

        tempValueString = thisApplication.GetValue(KeyName, DefaultValue).ToString()
        Return tempValueString

    End Function

    Private Sub InitializeQueueListeners()
        Try
            MainQueue = New MessageQueue(QueueLocationMachineName & "CRSReport_Main")
            CType(MainQueue.Formatter, XmlMessageFormatter).TargetTypeNames = New String() {"System.String"}

            ImmediateQueue = New MessageQueue(QueueLocationMachineName & "CRSReport_Immed")
            CType(ImmediateQueue.Formatter, XmlMessageFormatter).TargetTypeNames = New String() {"System.String"}

            ReturnQueue = New MessageQueue(QueueLocationMachineName & "CRSReport_Return")
            CType(ReturnQueue.Formatter, XmlMessageFormatter).TargetTypeNames = New String() {"System.String"}

            'AddHandler ImmediateQueue.ReceiveCompleted, New ReceiveCompletedEventHandler(AddressOf ReceiveImmediateReportRequest)
            'AddHandler MainQueue.ReceiveCompleted, New ReceiveCompletedEventHandler(AddressOf ReceiveMainReportRequest)

            If Not MessageQueue.Exists(Me.CRSReportEngineControlConsoleIn.Path) Then
                Me.CRSReportEngineControlConsoleIn = MessageQueue.Create(Me.CRSReportEngineControlConsoleIn.Path)
            End If

            ControlQueueIn = Me.CRSReportEngineControlConsoleIn
            ControlQueueIn.UseJournalQueue = False
            CType(ControlQueueIn.Formatter, XmlMessageFormatter).TargetTypeNames = New String() {"System.String"}

            'AddHandler ControlQueueIn.ReceiveCompleted, New ReceiveCompletedEventHandler(AddressOf ReceiveConsoleCommand)

            If Not MessageQueue.Exists(Me.CRSReportEngineControlConsoleOut.Path) Then
                Me.CRSReportEngineControlConsoleOut = MessageQueue.Create(Me.CRSReportEngineControlConsoleOut.Path)
            End If

            ControlQueueOut = Me.CRSReportEngineControlConsoleOut
            ControlQueueOut.UseJournalQueue = False
            CType(ControlQueueOut.Formatter, XmlMessageFormatter).TargetTypeNames = New String() {"System.String"}

        Catch ex As Exception

            CRSReportEngineEventLog.WriteEntry("Initialize Queue Listeners Error : " & ex.ToString())
            End
        End Try

    End Sub

    Private Sub KillPrivateQueues()
        'ControlQueueIn.Close()
        'System.Messaging.MessageQueue.Delete(".\Private$\CRSReportEngineControlConsoleIn")
        'ControlQueueOut.Close()
        'System.Messaging.MessageQueue.Delete(".\Private$\CRSReportEngineControlConsoleOut")
    End Sub

    Private Sub PeekAtReportQueues()
        Try
            ThisTimer.Stop()

            ReceiveConsoleCommand()

            If Me.MessageMode = MessageOutputMode.WriteMessage_Everything Then
                CRSReportEngineEventLog.WriteEntry("Peek At Queues - " & ControlChars.CrLf &
                "Immediate queue - " & ImmediateQueue.QueueName & " Status = " & ImmediateQueue.CanRead.ToString & ControlChars.CrLf &
                "Main Queue - " & MainQueue.QueueName & " Status = " & ImmediateQueue.CanRead.ToString)
            End If

            Dim IsQueueEmpty As Boolean = False
            Dim PeekMessage As New System.Messaging.Message()

            Try
                PeekMessage = ImmediateQueue.Peek(New TimeSpan(0))
            Catch e As MessageQueueException
                If e.MessageQueueErrorCode = MessageQueueErrorCode.IOTimeout Then
                    ' No message was in the queue.
                    IsQueueEmpty = True
                End If
                ' Handle other sources of MessageQueueException as necessary.
            Catch ex As Exception
                Throw ex
                ' Handle other exceptions as necessary.
            End Try

            If Not IsQueueEmpty Then
                Try
                    Dim ImmediateMessage As Message = ImmediateQueue.Receive(New TimeSpan(0, 0, 0, 0, HeartBeat))
                    If Not ImmediateMessage Is Nothing Then
                        ReceiveImmediateReportRequest(ImmediateMessage)
                    End If
                Catch msgex As MessageQueueException
                    If msgex.MessageQueueErrorCode = MessageQueueErrorCode.IOTimeout Then
                        ' No message was in the queue.
                        IsQueueEmpty = True
                    End If
                Catch ex As Exception
                    Throw ex
                End Try
            End If

            If IsQueueEmpty Then
                IsQueueEmpty = False
                Try
                    PeekMessage = MainQueue.Peek(New TimeSpan(0))
                Catch e As MessageQueueException
                    If e.MessageQueueErrorCode = MessageQueueErrorCode.IOTimeout Then
                        ' No message was in the queue.
                        IsQueueEmpty = True
                    End If
                    ' Handle other sources of MessageQueueException as necessary.
                Catch ex As Exception
                    Throw ex
                    ' Handle other exceptions as necessary.
                Finally
                    If Not IsNothing(PeekMessage) Then
                        PeekMessage.Dispose()
                    End If
                End Try

                If Not IsQueueEmpty Then
                    Try
                        Dim MainMessage As Message = MainQueue.Receive(New TimeSpan(0, 0, 0, 0, HeartBeat))
                        If Not MainMessage Is Nothing Then
                            ReceiveMainReportRequest(MainMessage)
                        End If
                        If Not IsNothing(MainMessage) Then
                            MainMessage.Dispose()
                        End If
                    Catch msgex As MessageQueueException
                        If msgex.MessageQueueErrorCode = MessageQueueErrorCode.IOTimeout Then
                            ' No message was in the queue.
                            IsQueueEmpty = True
                        End If
                    Catch ex As Exception
                        Throw ex
                    End Try
                End If
            End If
        Catch msgex As MessageQueueException
            CRSReportEngineEventLog.WriteEntry("Peek At Queues : " & [Enum].GetName(Type.GetType("System.Messaging.MessageQueueErrorCode"), msgex.MessageQueueErrorCode))
        Catch ex As Exception
            CRSReportEngineEventLog.WriteEntry("Peek At Queues : " & ex.ToString())
            Throw ex
            End
        Finally
            ThisTimer.Start()
        End Try
    End Sub

    Sub ReceiveConsoleCommand()
        Try

            Dim ConsoleMessage As Message = ControlQueueIn.Receive(New TimeSpan(0, 0, 1))

            Dim CommandText As String = String.Empty

            Try
                CommandText = ConsoleMessage.Body.ToString().Split(" "c)(0)
            Catch
            End Try

            Select Case CommandText
                Case "List"
                    WriteListMessage()
                Case "Stop"
                    Me.OnStop()
                    End
                Case "Status"
                    WriteStatusMessage()
                Case "Abort" 'Usage: "Abort 171 username"
                    Dim ThreadHash As Integer
                    Dim Caller As String = String.Empty

                    Try
                        Try
                            ThreadHash = Integer.Parse(ConsoleMessage.Body.ToString().Split(" "c)(1))
                            Caller = ConsoleMessage.Body.ToString().Split(" "c)(2)
                        Catch
                        End Try

                        Dim MessageBody As New System.Text.StringBuilder
                        Dim theMessage As New System.Messaging.Message
                        theMessage.Formatter = New Messaging.XmlMessageFormatter
                        MessageBody.Append("<abortreply ThreadID='" & ThreadHash.ToString("0") & "'>")

                        If WorkerList.KillWorkerBee(ThreadHash, "ReceiveConsoleCommand_Abort from " & Caller) Then
                            MessageBody.Append("Aborted Successfully!")
                        Else
                            MessageBody.Append("Abort Unsuccessful.")
                        End If

                        MessageBody.Append("</abortreply>")

                        theMessage.Body = MessageBody.ToString()
                        theMessage.Priority = MessagePriority.High
                        ControlQueueOut.Send(theMessage)
                    Catch ex As Exception
                        Throw ex
                    End Try
                Case ""
                Case Else
            End Select

        Catch msgex As MessageQueueException
            If msgex.MessageQueueErrorCode = MessageQueueErrorCode.IOTimeout Then
                Exit Sub
            Else
                Throw msgex
            End If
        Catch ex As Exception
            CRSReportEngineEventLog.WriteEntry("Receive Console Command : " & ex.ToString())
            Throw ex
            End
        End Try
    End Sub

    Sub ReceiveReportRequest(ByVal ReportRequestMessage As Message, ByVal priority As MessagePriority)

        Try
            If MessageMode > MessageOutputMode.WriteMessages_Off Then
                CRSReportEngineEventLog.WriteEntry([Enum].GetName(priority.GetType, priority) & " priority request received. " & vbCrLf & "ID=" & ReportRequestMessage.Id & vbCrLf & "------------------" & vbCrLf & ReportRequestMessage.Body.ToString)
            End If

            WorkerList.AddWorkerBee()
            StartInstance(ReportRequestMessage.Id, ReportRequestMessage.Label, ReportRequestMessage.Body.ToString(), CType(priority, ReportPriorityList))
        Catch ex As Exception
            CRSReportEngineEventLog.WriteEntry("Receive Report Request Error : " & ex.ToString())
            Throw ex
            End
        End Try
    End Sub

    Sub ReceiveMainReportRequest(ByVal MainReportRequestMessage As Message)

        ReceiveReportRequest(MainReportRequestMessage, CType(ReportPriorityList.Normal, System.Messaging.MessagePriority))

    End Sub

    Sub ReceiveImmediateReportRequest(ByVal ImmediateReportRequestMessage As Message)

        ReceiveReportRequest(ImmediateReportRequestMessage, CType(ReportPriorityList.High, System.Messaging.MessagePriority))

    End Sub

    Sub WriteStatusMessage()
        Dim theMessage As New System.Messaging.Message()
        theMessage.Body = "<statusreport/>"
        theMessage.Priority = MessagePriority.High
        ControlQueueOut.Send(theMessage)
    End Sub

    Sub WriteListMessage()
        Try

            Dim MessageBody As New System.Text.StringBuilder()
            Dim theMessage As New System.Messaging.Message()
            theMessage.Formatter = New Messaging.XmlMessageFormatter()
            MessageBody.Append("<reportlist>")

            Dim ValueList As ICollection = WorkerList.WorkerList.Values

            Dim ObjectArray() As Object

            With MessageBody
                For Each ObjectArray In ValueList

                    Dim ReportInfoObject As ReportInfo = CType(ObjectArray(1), ReportInfo)
                    Dim ThreadObject As Thread = CType(ObjectArray(0), Thread)
                    .Append("<report>")

                    .Append("<messageid>")
                    .Append(ReportInfoObject.MessageID)
                    .Append("</messageid>")

                    .Append("<reportname>")
                    .Append(ReportInfoObject.ReportName)
                    .Append("</reportname>")

                    .Append("<priority>")
                    .Append([Enum].GetName(GetType(ReportInfo.ReportPriority), ReportInfoObject.Priority))
                    .Append("</priority>")

                    .Append("<threadstatus>")
                    .Append([Enum].GetName(GetType(System.Threading.ThreadState), ThreadObject.ThreadState))
                    .Append("</threadstatus>")

                    .Append("<threadhashcode>")
                    .Append(ThreadObject.GetHashCode.ToString())
                    .Append("</threadhashcode>")

                    .Append("<status>")
                    .Append(ReportInfoObject.Status)
                    .Append("</status>")

                    .Append("<requestxml>")
                    .Append(ReportInfoObject.ReportRequestXML.OuterXml)
                    .Append("</requestxml>")

                    .Append("<starttime>")
                    .Append(ReportInfoObject.StartTime.ToString())
                    .Append("</starttime>")

                    .Append("<attemptcount>")
                    .Append(ReportInfoObject.AttemptCount.ToString())
                    .Append("</attemptcount>")

                    .Append("</report>")

                Next
                .Append("</reportlist>")
            End With
            theMessage.Body = MessageBody.ToString()
            theMessage.Priority = MessagePriority.High
            ControlQueueOut.Send(theMessage)
        Catch Ex As Exception
            CRSReportEngineEventLog.WriteEntry("Write List Message Error : " & Ex.ToString())
            'Throw Ex
        End Try
    End Sub

    Private Sub StartInstance(ByVal MessageID As String, ByVal MessageLabel As String, ByVal MessageBody As String, ByVal MessagePriority As ReportPriorityList)
        Try
            Dim xmlBodyDoc As New Xml.XmlDocument()

            Try
                xmlBodyDoc.LoadXml(MessageBody)
            Catch ex As XmlException
                CRSReportEngineEventLog.WriteEntry("XML Error : " & ex.ToString())
            Catch ex2 As Exception
                CRSReportEngineEventLog.WriteEntry("StartInstance Error : " & ex2.ToString())
            End Try
            RptRunner = New ReportRunner()

            With RptRunner
                .AdminConnectionString = CRSAdminConnectionString
                .CompanyDatabaseConnectionString = CompanyDatabaseConnectionString
                .ConnectionString = ConnectionString
                .CommandTimeout = CommandTimeOut
                .ReportPath = ReportPath
                .ExportPath = ExportPath
                .XMLReportRequest = xmlBodyDoc

                AddHandler .CheckingProperties, AddressOf CheckingProperties
                AddHandler .LoadingReportInfo, AddressOf LoadingReportInfo
                AddHandler .SetReportName, AddressOf ReportNameSet
                AddHandler .SettingCommandParameters, AddressOf SettingCommandParameters
                AddHandler .SettingCommandProperties, AddressOf SettingCommandProperties
                AddHandler .FillDataSetInProgress, AddressOf FillDataSetInProgress
                AddHandler .FillDataSetComplete, AddressOf FillDataSetComplete
                AddHandler .LoadReportFileInProgress, AddressOf LoadReportFileInProgress
                AddHandler .LoadReportFileComplete, AddressOf LoadReportFileComplete
                AddHandler .SettingReportParameters, AddressOf SettingReportParameters
                AddHandler .SettingExportOptions, AddressOf SettingExportOptions
                AddHandler .ExportInProgress, AddressOf ExportInProgress
                AddHandler .ExportComplete, AddressOf RptRunner_ExportComplete
                AddHandler .ExportError, AddressOf RptRunner_ExportError
                AddHandler .ExportCustomError, AddressOf RptRunner_ExportCustomError
                AddHandler .LogGenericMessage, AddressOf LogGenericMessage

            End With

            Dim ReportingThreadStartFunction As New ThreadStart(AddressOf RptRunner.RunReport)

            Dim ReportingThread As New Thread(ReportingThreadStartFunction)

            ReportingThread.Name = ReportingThread.GetHashCode().ToString

            Debug.WriteIf(MessageMode > MessageOutputMode.WriteMessages_Off, "Thread - " & ReportingThread.GetHashCode())

            Dim ReportName As String = ""
            Try
                With xmlBodyDoc
                    Dim ReportNameNode As XmlNode = .DocumentElement.SelectSingleNode("reportrequest/reportname")
                    If Not ReportNameNode Is Nothing Then
                        ReportName = ReportNameNode.InnerText
                    End If
                End With
            Catch
            End Try

            Select Case MessagePriority
                Case ReportPriorityList.Normal
                    ReportingThread.Priority = ThreadPriority.BelowNormal
                    WorkerList.Add(ReportingThread.GetHashCode(), New Object(2) {ReportingThread, New ReportInfo(MessageID, MessageLabel, ReportName, ReportingThread.GetHashCode(), ReportInfo.ReportPriority.Normal, xmlBodyDoc), RptRunner})
                Case ReportPriorityList.High
                    ReportingThread.Priority = ThreadPriority.Normal
                    WorkerList.Add(ReportingThread.GetHashCode(), New Object(2) {ReportingThread, New ReportInfo(MessageID, MessageLabel, ReportName, ReportingThread.GetHashCode(), ReportInfo.ReportPriority.High, xmlBodyDoc), RptRunner})
            End Select

            'RunningReportCounter.Increment()
            ReportingThread.Start()

            If MessageMode > MessageOutputMode.WriteMessages_Off Then
                CRSReportEngineEventLog.WriteEntry("Thread Started - " & WorkerList.TotalWorkingThreads)
            End If

            xmlBodyDoc = Nothing
        Catch ex As Exception
            CRSReportEngineEventLog.WriteEntry("Write List Message Error : " & Ex.ToString())
            'Throw Ex
        End Try

    End Sub

#End Region

End Class
