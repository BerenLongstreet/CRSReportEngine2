Option Strict On

Imports System.Threading
Imports System.Runtime.CompilerServices

Public Class WorkerList
    Private _InternalList As Hashtable
    Private _TotalWorkingThreads As Integer
    Private Coordinator As New Mutex(False, "CRSReportEngineWorkerListMutex")

    Public ReadOnly Property WorkerList() As Hashtable
        Get
            Return _InternalList
        End Get
    End Property

    Public ReadOnly Property TotalWorkingThreads() As Integer
        Get
            Return _TotalWorkingThreads
        End Get
    End Property

    Public Sub New()

        _InternalList = New Hashtable()

    End Sub

    <MethodImpl(MethodImplOptions.Synchronized)> _
    Public Sub RemoveWorkerBee(ByVal ThreadHash As Integer, ByVal Caller As String)
        Try
            Coordinator.WaitOne()
            Try
                _InternalList.Remove(ThreadHash)
                Debug.WriteLine("Thread #" & ThreadHash.ToString() & " out called by " & Caller)
            Catch
            Finally
                Interlocked.Decrement(_TotalWorkingThreads)
            End Try
        Catch ex As ThreadInterruptedException
            Exit Sub
        Finally
            Coordinator.ReleaseMutex()
        End Try
    End Sub

    <MethodImpl(MethodImplOptions.Synchronized)> _
    Public Function KillWorkerBee(ByVal ThreadHash As Integer, ByVal Caller As String) As Boolean
        Try
            Coordinator.WaitOne()
            Try
                Dim WorkerBeeThread As System.Threading.Thread = DirectCast(DirectCast(_InternalList.Item(ThreadHash), Object())(0), System.Threading.Thread)
                Try
                    WorkerBeeThread.Abort()

                    _InternalList.Remove(ThreadHash)

                    Debug.WriteLine("Thread #" & ThreadHash.ToString() & " out called by " & Caller)

                    Return True

                Catch ThreadEx As ThreadAbortException
                Catch ex As Exception
                    Throw ex
                Finally
                End Try
            Catch
            Finally
                Interlocked.Decrement(_TotalWorkingThreads)
            End Try
        Catch IntEx As ThreadInterruptedException
            Exit Function
        Catch OtherEx As Exception
            Return False
        Finally
            Coordinator.ReleaseMutex()
        End Try
    End Function

    Public Sub AddWorkerBee()
        Try

            Interlocked.Increment(_TotalWorkingThreads)
        Catch ex As Exception
            Throw ex
        End Try
    End Sub

    Public Sub ChangeReportName(ByVal ThreadHash As Integer, ByVal newReportName As String)
        Try
            Coordinator.WaitOne()
            CType(CType(_InternalList.Item(ThreadHash), Object())(1), ReportInfo).ReportName = newReportName
        Catch ex As ThreadInterruptedException
            Exit Sub
        Finally
            Coordinator.ReleaseMutex()
        End Try
    End Sub

    Public Sub ChangeAttemptCount(ByVal ThreadHash As Integer, ByVal AttemptCount As Short)
        Try
            Coordinator.WaitOne()
            If Not IsNothing(_InternalList.Item(ThreadHash)) Then
                CType(CType(_InternalList.Item(ThreadHash), Object())(1), ReportInfo).AttemptCount = AttemptCount
            End If
        Catch threadEx As ThreadInterruptedException
            Exit Sub
        Catch ex As Exception
            Throw ex
        Finally
            Coordinator.ReleaseMutex()
        End Try

    End Sub

    Public Sub ChangeStatusText(ByVal ThreadHash As Integer, ByVal StatusText As String)
        Try
            Coordinator.WaitOne()
            If Not IsNothing(_InternalList.Item(ThreadHash)) Then
                CType(CType(_InternalList.Item(ThreadHash), Object())(1), ReportInfo).Status = StatusText
            End If
        Catch threadEx As ThreadInterruptedException
            Exit Sub
        Catch ex As Exception
            Throw ex
        Finally
            Coordinator.ReleaseMutex()
        End Try
    End Sub

    Public Function GetWorkerListReportInfo(ByVal ThreadHash As Integer) As ReportInfo
        Try
            Coordinator.WaitOne()
            Dim ThisWorkerItemArray() As Object
            ThisWorkerItemArray = CType(_InternalList.Item(ThreadHash), Object())
            If Not IsNothing(ThisWorkerItemArray) Then
                Return CType(ThisWorkerItemArray(1), ReportInfo)
            Else
                Return Nothing
            End If
        Catch threadEx As ThreadInterruptedException
            Return Nothing
        Catch ex As Exception
            Throw ex
        Finally
            Coordinator.ReleaseMutex()
        End Try

    End Function

    Public Function GetWorkerListReportThread(ByVal ThreadHash As Integer) As Thread
        Try
            Coordinator.WaitOne()
            Dim ThisWorkerItemArray() As Object = CType(_InternalList.Item(ThreadHash), Object())
            Return CType(ThisWorkerItemArray(0), Thread)
        Catch threadEx As ThreadInterruptedException
            Return Nothing
        Catch ex As Exception
            Throw ex
        Finally
            Coordinator.ReleaseMutex()
        End Try

    End Function

    Public Sub Add(ByVal Key As Object, ByVal Value As Object)
        Try
            Coordinator.WaitOne()
            _InternalList.Add(Key, Value)
            Debug.WriteLine("Thread #" & Key.ToString() & " in.")
        Catch threadEx As ThreadInterruptedException
            Exit Sub
        Catch ex As Exception
            Throw ex
        Finally
            Coordinator.ReleaseMutex()
        End Try
    End Sub

End Class
