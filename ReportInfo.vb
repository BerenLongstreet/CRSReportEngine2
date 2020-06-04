Public Class ReportInfo
    Private _MessageID As String
    Private _MessageLabel As String
    Private _ReportName As String
    Private _ThreadHash As Integer
    Private _Priority As ReportPriority
    Private _ReportRequestXML As Xml.XmlDocument
    Private _StartTime As DateTime
    Private _Status As String
    Private _AttemptCount As Short

    Public Enum ReportPriority As Short
        Normal = 3
        High = 5
    End Enum

    Public Property MessageID() As String
        Get
            Return _MessageID
        End Get
        Set(ByVal Value As String)
            _MessageID = Value
        End Set
    End Property

    Public Property MessageLabel() As String
        Get
            Return _MessageLabel
        End Get
        Set(ByVal Value As String)
            _MessageLabel = Value
        End Set
    End Property

    Public Property ReportName() As String
        Get
            Return _ReportName
        End Get
        Set(ByVal Value As String)
            _ReportName = Value
        End Set
    End Property

    Public Property ThreadHash() As Integer
        Get
            Return _ThreadHash
        End Get
        Set(ByVal Value As Integer)
            _ThreadHash = Value
        End Set
    End Property

    Public Property Priority() As ReportPriority
        Get
            Return _Priority
        End Get
        Set(ByVal Value As ReportPriority)
            _Priority = Value
        End Set
    End Property

    Public Property ReportRequestXML() As Xml.XmlDocument
        Get
            Return _ReportRequestXML
        End Get
        Set(ByVal Value As Xml.XmlDocument)
            _ReportRequestXML = Value
        End Set
    End Property

    Public Property StartTime() As DateTime
        Get
            Return _StartTime
        End Get
        Set(ByVal Value As DateTime)
            _StartTime = Value
        End Set
    End Property

    Public Property Status() As String
        Get
            Return _Status
        End Get
        Set(ByVal Value As String)
            _Status = Value
        End Set
    End Property

    Public Property AttemptCount() As Short
        Get
            Return _AttemptCount
        End Get
        Set(ByVal Value As Short)
            _AttemptCount = Value
        End Set
    End Property

    Public Sub New(ByVal newMessageID As String, ByVal newMessageLabel As String, ByVal newReportName As String, ByVal newThreadHash As Integer, ByVal newReportPriority As ReportPriority, ByVal newReportRequestXML As Xml.XmlDocument)
        With Me
            .StartTime = DateTime.Now()
            .MessageID = newMessageID
            .MessageLabel = newMessageLabel
            .ReportName = newReportName
            .ThreadHash = newThreadHash
            .Priority = newReportPriority
            .ReportRequestXML = newReportRequestXML
            .Status = "Not Started"
            .AttemptCount = 1S
        End With
    End Sub
End Class
