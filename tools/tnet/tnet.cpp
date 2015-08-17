// main.cpp - tnet
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Declarations
*
***/

enum {
    kExitBadArgs = 1,
    kExitConnectFailed = 2,
    kExitDisconnect = 3,
    kExitCtrlBreak = 4,
};


/****************************************************************************
*
*   Variables
*
***/

static WORD s_consoleAttrs;
static SockAddr s_localAddr;


/****************************************************************************
*
*   Helpers
*
***/

//===========================================================================
static BOOL WINAPI ControlCallback (DWORD ctrl) {
    switch (ctrl) {
        case CTRL_C_EVENT:
        case CTRL_BREAK_EVENT:
            DimAppSignalShutdown(kExitCtrlBreak);
            return true;
    }

    return false;
}

//===========================================================================
static void InitializeConsole () {
    // set ctrl-c handler
    SetConsoleCtrlHandler(&ControlCallback, true);

    // disable echo
    HANDLE hInput = GetStdHandle(STD_INPUT_HANDLE);
    SetConsoleMode(hInput, ENABLE_PROCESSED_INPUT);

    // save console text attributes
    HANDLE hOutput = GetStdHandle(STD_OUTPUT_HANDLE);
    CONSOLE_SCREEN_BUFFER_INFO info;
    if (!GetConsoleScreenBufferInfo(hOutput, &info)) {
        DimLog{kCrash} << "GetConsoleScreenBufferInfo: " << GetLastError();
    }
    s_consoleAttrs = info.wAttributes;
}

//===========================================================================
static void SetConsoleText (WORD attr) {
    HANDLE hOutput = GetStdHandle(STD_OUTPUT_HANDLE);
    SetConsoleTextAttribute(hOutput, attr);
}


/****************************************************************************
*
*   SocketConn
*
***/

class SocketConn 
    : public IDimSocketNotify 
    , public IDimAddressNotify
{
    // IDimSocketNotify
    void OnSocketConnect (const DimSocketConnectInfo & info) override;
    void OnSocketConnectFailed () override;
    void OnSocketRead (const DimSocketData & data) override;
    void OnSocketDisconnect () override;

    // IDimAddressNotify
    void OnAddressFound (SockAddr * addrs, int count) override;
};
static SocketConn s_socket;

//===========================================================================
void SocketConn::OnAddressFound (SockAddr * addrs, int count) {
    if (!count) {
        cout << "Host not found" << endl;
        DimAppSignalShutdown(kExitConnectFailed);
    } else {
        cout << "Connecting on " << s_localAddr << " to " << *addrs << endl;
        DimSocketConnect(this, *addrs, s_localAddr);
    }
}

//===========================================================================
void SocketConn::OnSocketConnect (const DimSocketConnectInfo & info) {
    SetConsoleText(FOREGROUND_GREEN | FOREGROUND_INTENSITY);
    cout << "Connected" << endl;
}

//===========================================================================
void SocketConn::OnSocketConnectFailed () {
    cout << "Connect failed" << endl;
    DimAppSignalShutdown(kExitConnectFailed);
}

//===========================================================================
void SocketConn::OnSocketRead (const DimSocketData & data) {
    cout.write(data.data, data.bytes);
    cout.flush();
}

//===========================================================================
void SocketConn::OnSocketDisconnect () {
    DimAppSignalShutdown(kExitDisconnect);
}


/****************************************************************************
*
*   ConsoleReader
*
***/

class ConsoleReader : public IDimFileNotify {
public:
    unique_ptr<DimSocketBuffer> m_buffer;
    unique_ptr<IDimFile> m_file;

    bool QueryDestroy () const { return !m_file && !m_buffer; }

    bool OnFileRead (
        char * data, 
        int bytes,
        int64_t offset,
        IDimFile * file
    ) override;
    void OnFileEnd (
        int64_t offset, 
        IDimFile * file
    ) override;
};
static ConsoleReader s_console;

//===========================================================================
bool ConsoleReader::OnFileRead (
    char * data, 
    int bytes,
    int64_t offset,
    IDimFile * file
) {
    DimSocketWrite(&s_socket, move(m_buffer), bytes);
    // stop reading (return false) so we can get a new buffer
    return false;
}

//===========================================================================
void ConsoleReader::OnFileEnd (int64_t offset, IDimFile * file) {
    if (m_file) {
        m_buffer = DimSocketGetBuffer();
        DimFileRead(
            this, 
            m_buffer->data,
            m_buffer->len,
            file
        );
    } else {
        m_buffer.reset();
    }
}


/****************************************************************************
*
*   MainShutdown
*
***/

class MainShutdown : public IDimAppShutdownNotify {
    void OnAppStartClientCleanup () override;
    bool OnAppQueryClientDestroy () override;
    void OnAppStartConsoleCleanup () override;
};

//===========================================================================
void MainShutdown::OnAppStartClientCleanup () {
    s_console.m_file.reset();
    DimSocketDisconnect(&s_socket);
}

//===========================================================================
bool MainShutdown::OnAppQueryClientDestroy () {
    if (DimSocketGetMode(&s_socket) != kRunStopped 
        || !s_console.QueryDestroy()
    ) {
        return DimQueryDestroyFailed();
    }
    return true;
}

//===========================================================================
void MainShutdown::OnAppStartConsoleCleanup () {
    if (s_consoleAttrs)
        SetConsoleText(s_consoleAttrs);
}


/****************************************************************************
*
*   main
*
***/

//===========================================================================
void Start (int argc, char * argv[]) {
    if (argc < 2) {
        cout << "tnet v1.0 (" __DATE__ ")\n"
            << "usage: tnet <remote address> [<local address>]\n";
        return DimAppSignalShutdown(kExitBadArgs);
    }

    InitializeConsole();

    if (argc > 2)
        Parse(&s_localAddr, argv[2], 0);

    int cancelId;
    DimAddressQuery(&cancelId, &s_socket, argv[1], 23);
    
    DimFileOpen(s_console.m_file, "conin$", IDimFile::kReadWrite);
    s_console.m_buffer = DimSocketGetBuffer();
    DimFileRead(
        &s_console, 
        s_console.m_buffer->data,
        s_console.m_buffer->len,
        s_console.m_file.get()
    );
}

//===========================================================================
int main(int argc, char * argv[]) {
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
    _set_error_mode(_OUT_TO_MSGBOX);

    MainShutdown cleanup;
    DimAppInitialize();
    DimAppMonitorShutdown(&cleanup);

    Start(argc, argv);    

    int code = DimAppWaitForShutdown();
    return code;
}
