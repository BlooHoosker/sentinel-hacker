#ifndef __PROGTEST__
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <climits>
#include <cfloat>
#include <cassert>
#include <cmath>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <vector>
#include <set>
#include <list>
#include <map>
#include <unordered_set>
#include <unordered_map>
#include <queue>
#include <stack>
#include <deque>
#include <memory>
#include <functional>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include "progtest_solver.h"
#include "sample_tester.h"
using namespace std;
#endif /* __PROGTEST__ */

class CSentinelHacker
{
  public:
    static bool              SeqSolve                      ( const vector<uint64_t> & fragments,
                                                             CBigInt         & res );
    void                     AddTransmitter                ( ATransmitter      x );
    void                     AddReceiver                   ( AReceiver         x );
    void                     AddFragment                   ( uint64_t          x );
    void                     Start                         ( unsigned          thrCount );
    void                     Stop                          ( void );
    void Receiver(AReceiver receiver);
    void Transmitter(ATransmitter transmitter);
    void WorkerThread();

private:
    uint32_t GetID(uint64_t fragment);

    unordered_map<uint32_t, vector<uint64_t>> fragmentsDB;

    queue<uint64_t> fragmentsReceived;
    queue<pair<uint32_t, CBigInt>> resultsToSend;

    vector<ATransmitter> transmiters;
    vector<AReceiver> receivers;

    vector<thread> threadsRec;
    vector<thread> threadsTrans;
    vector<thread> threadsWorker;

    mutex DB_mutex;
    mutex rec_mutex;
    mutex trans_mutex;
    condition_variable cond_1;
    condition_variable cond_2;

    int receiverCnt;
    int workerCnt;
    bool stop;
};

bool CSentinelHacker::SeqSolve(const vector<uint64_t> &fragments, CBigInt &res) {
    CBigInt result;

    uint32_t perm_found = FindPermutations(&fragments[0], fragments.size(),
            [&result](const uint8_t * arr, size_t cnt){
                    CBigInt tmp;
                    tmp = CountExpressions(arr+4, cnt-32);
                    if (tmp.CompareTo(result) > 0){
                        result = tmp;
                    }
            });

    if (perm_found == 0){
        return false;
    }

    res = result;
    return true;
}

void CSentinelHacker::AddTransmitter(ATransmitter x) {
    transmiters.push_back(x);
}

void CSentinelHacker::AddReceiver(AReceiver x) {
    receivers.push_back(x);
}

void CSentinelHacker::Start(unsigned thrCount) {

    receiverCnt = receivers.size();

    for (auto &rec : receivers){
        threadsRec.emplace_back(&CSentinelHacker::Receiver, this, rec);
    }

    workerCnt = thrCount;

    for (int i = 0; i < workerCnt; i++){
        threadsWorker.emplace_back(&CSentinelHacker::WorkerThread, this);
    }


    for (auto &trans : transmiters){
        threadsTrans.emplace_back(&CSentinelHacker::Transmitter, this, trans);
    }
}

void CSentinelHacker::Stop(void) {

    unique_lock<mutex> ulock (rec_mutex);
    stop = true;
    ulock.unlock();

    for (auto & t : threadsRec){
        t.join();

    }
    cond_1.notify_all();

    for (auto &t : threadsWorker){
        t.join();
    }
    cond_2.notify_all();

    for (auto &t : threadsTrans){
        t.join();
    }
}

void CSentinelHacker::AddFragment(uint64_t x) {

    /*
     * Locks fragments queue
     * Pushes new fragment in the queue
     * If queue was empty it will try to wake up a waiting thread
     * Ulocks fragments queue
     */

    unique_lock<mutex> ulock (rec_mutex);
    if (fragmentsReceived.empty()){
        fragmentsReceived.push(x);
        cond_1.notify_all();
    } else {
        fragmentsReceived.push(x);
    }
    ulock.unlock();
}

void CSentinelHacker::Receiver(AReceiver receiver) {

    uint64_t fragment = 0;

    // Cycle in which fragments are being added to the fragment queue. Queue blocking is handled by AddFragment
    while (receiver->Recv(fragment)){
        AddFragment(fragment);
    }
    unique_lock<mutex> ulock (rec_mutex);
    receiverCnt--;
    if (receiverCnt == 0 ){
        cond_1.notify_all();
    }
    ulock.unlock();
    cout<<"receiving finished-------------------------------------------"<<endl;
}

void CSentinelHacker::WorkerThread() {

    uint64_t fragment_tmp = 0;
    uint32_t id_tmp = 0;
    CBigInt result;
    vector<uint64_t > message;
    unique_lock<mutex> ulock_2 (trans_mutex, std::defer_lock);

    while (1){
        /*
        unique_lock<mutex> ulock_1 (rec_mutex);
        unique_lock<mutex> ulock_2 (trans_mutex);
        if (stop && fragmentsReceived.empty() && receiverCnt == 0){
            workerCnt--;
            ulock_2.unlock();
            ulock_1.unlock();
            return;
        } else {
            ulock_2.unlock();
            if (fragmentsReceived.empty()){
                cond_1.wait(ulock_1, [this, &end]{
                    if (fragmentsReceived.empty()){
                        return receiverCnt == 0 && stop;
                    }
                    return true;
                });
                if (stop && fragmentsReceived.empty() && receiverCnt == 0){
                    ulock_1.unlock();
                    return;
                }
            }
        }
        fragment_tmp = fragmentsReceived.front();
        fragmentsReceived.pop();
        ulock_1.unlock();
        */

        unique_lock<mutex> ulock_1 (rec_mutex);
        // If theres nothing in the queue then see what it has to do. Otherwise can just go process stuff
        if (fragmentsReceived.empty()){
            // If no work can be received anymore aka queue empty, stop called, no receivers then end safely
            if (stop && receiverCnt <= 0){
                ulock_2.lock();
                workerCnt--;
                ulock_2.unlock();
                ulock_1.unlock();
                return;
            }
            // If queue is empty but work might come at some point then wait
            //cout<<"Worker going to sleep"<<endl;
            cond_1.wait(ulock_1, [this]{
                // if queue is empty check if work can come at some point
                if (fragmentsReceived.empty()){
                    return receiverCnt == 0 && stop;
                }
                return true;
                });
            //cout<<"Worker awaking"<<endl;
            // If no work can be received anymore aka queue empty, stop called, no receivers then end safely
            if (stop && fragmentsReceived.empty() && receiverCnt == 0){
                ulock_2.lock();
                workerCnt--;
                ulock_2.unlock();
                ulock_1.unlock();
                return;
            }
        }

        fragment_tmp = fragmentsReceived.front();
        fragmentsReceived.pop();
        ulock_1.unlock();

        // Gets ID of the fragment
        id_tmp = GetID(fragment_tmp);

        unique_lock<mutex> ulock_db (DB_mutex);
        // Searches for message with this ID, if it doesnt find it it will create new entry, and then add it to the vector
        fragmentsDB[id_tmp].push_back(fragment_tmp);

        // Copies message with ID and with added fragment
        message = fragmentsDB[id_tmp];
        ulock_db.unlock();

        // Processes the message, if it succeeds it will push result in to result queue
        if (SeqSolve(message, result)){
            ulock_2.lock();
            resultsToSend.push(make_pair(id_tmp, result));
            cond_2.notify_all();
            ulock_2.unlock();

            ulock_db.lock();
            fragmentsDB.erase(id_tmp);
            ulock_db.unlock();
        }
    }

}

void CSentinelHacker::Transmitter(ATransmitter transmitter) {

    pair<uint32_t, CBigInt> result;
    uint32_t id_tmp;

    while (1){

        /*
        unique_lock<mutex> ulock (trans_mutex);
        if (workerCnt == 0 && resultsToSend.empty()){
            ulock.unlock();
            break;
        } else if (resultsToSend.empty()){
            cond_2.wait(ulock, [this, &end]{
                if (resultsToSend.empty()){
                    return workerCnt == 0;
                }
                return true;
            });
            if (workerCnt == 0 && resultsToSend.empty()) {
                ulock.unlock();
                break;
            }
        }
        result = resultsToSend.front();
        resultsToSend.pop();
        ulock.unlock();
        transmitter->Send(result.first, result.second);
        */
        unique_lock<mutex> ulock_1 (trans_mutex);
        // If theres nothing in the queue then see what it has to do. Otherwise can just go process stuff
        if (resultsToSend.empty()){
            // If no work can be received anymore aka queue empty, no workers then end safely
            if (workerCnt == 0 ){
                ulock_1.unlock();
                break;
            }
            // If queue is empty but work might come at some point then wait
            cond_2.wait(ulock_1, [this]{
                // if queue is empty check if work can come at some point
                if (resultsToSend.empty()){
                    return workerCnt == 0;
                }
                return true;
            });
            // If no work can be received anymore aka queue empty, no workers then end safely
            if (workerCnt == 0 && resultsToSend.empty()) {
                ulock_1.unlock();
                break;
            }
        }
        result = resultsToSend.front();
        resultsToSend.pop();
        ulock_1.unlock();
        transmitter->Send(result.first, result.second);
    }
    while(1){

        unique_lock<mutex> ulock_2 (DB_mutex);
        if (fragmentsDB.empty()){
            ulock_2.unlock();
            return;
        }
        auto it = fragmentsDB.begin();
        id_tmp = it->first;
        fragmentsDB.erase(it);
        ulock_2.unlock();
        transmitter->Incomplete(id_tmp);
    }
}

uint32_t CSentinelHacker::GetID(const uint64_t fragment) {
    uint64_t tmp = fragment;
    tmp = tmp >> 37;
    return tmp;
}


// TODO: CSentinelHacker implementation goes here
//-------------------------------------------------------------------------------------------------
#ifndef __PROGTEST__
int                main                                    ( void )
{
  using namespace std::placeholders;
  for ( const auto & x : g_TestSets )
  { 
    CBigInt res;
    assert ( CSentinelHacker::SeqSolve ( x . m_Fragments, res ) );
    assert ( CBigInt ( x . m_Result ) . CompareTo ( res ) == 0 );
  }

  CSentinelHacker test;
  auto            trans = make_shared<CExampleTransmitter> ();
  AReceiver       recv  = make_shared<CExampleReceiver> ( initializer_list<uint64_t> { 0x02230000000c, 0x071e124dabef, 0x02360037680e, 0x071d2f8fe0a1, 0x055500150755 } );
  
  test . AddTransmitter ( trans ); 
  test . AddReceiver ( recv ); 
  test . Start ( 3 );
  
  static initializer_list<uint64_t> t1Data = { 0x071f6b8342ab, 0x0738011f538d, 0x0732000129c3, 0x055e6ecfa0f9, 0x02ffaa027451, 0x02280000010b, 0x02fb0b88bc3e };
  thread t1 ( FragmentSender, bind ( &CSentinelHacker::AddFragment, &test, _1 ), t1Data );
  
  static initializer_list<uint64_t> t2Data = { 0x073700609bbd, 0x055901d61e7b, 0x022a0000032b, 0x016f0000edfb };
  thread t2 ( FragmentSender, bind ( &CSentinelHacker::AddFragment, &test, _1 ), t2Data );
  FragmentSender ( bind ( &CSentinelHacker::AddFragment, &test, _1 ), initializer_list<uint64_t> { 0x017f4cb42a68, 0x02260000000d, 0x072500000025 } );
  t1 . join ();
  t2 . join ();
  test . Stop ();
  assert ( trans -> TotalSent () == 4 );
  assert ( trans -> TotalIncomplete () == 2 );
  return 0;  
}
#endif /* __PROGTEST__ */ 
