#include "duckdb.hpp"
#include <iostream>
#include <fstream>
#include <thread>
#include <vector>
#include <chrono>
#include <mutex>
#include <string>
#include <random>
#include <atomic>

using namespace duckdb;
using namespace std::chrono;

struct BenchResult {
    std::string scenario;
    std::string query_type;
    double latency_microseconds;
};

// Global control flag for the benchmark duration
std::atomic<bool> keep_running(true);
std::mutex file_mutex;

void run_elephant(DuckDB &db, std::string scenario_name) {
    Connection con(db);
    std::vector<BenchResult> local_results;

    // TPC-H Q1: full lineitem scan with 6 aggregations (official elephant query)
    std::string elephant_query = R"(
        SELECT
            l_returnflag,
            l_linestatus,
            sum(l_quantity) AS sum_qty,
            sum(l_extendedprice) AS sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            avg(l_quantity) AS avg_qty,
            avg(l_extendedprice) AS avg_price,
            avg(l_discount) AS avg_disc,
            count(*) AS count_order
        FROM lineitem
        WHERE l_shipdate <= CAST('1998-09-02' AS date)
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus;
    )";
    
    while (keep_running) {
        auto start = high_resolution_clock::now();
        con.Query(elephant_query);
        auto end = high_resolution_clock::now();
        
        double duration = duration_cast<microseconds>(end - start).count();
        local_results.push_back({scenario_name, "Elephant", duration});
    }

    // Write to file only after the run finishes to avoid lock contention during benchmarking
    std::lock_guard<std::mutex> lock(file_mutex);
    std::ofstream csv_file("mlfq_latency_results.csv", std::ios_base::app);
    for (const auto& res : local_results) {
        csv_file << res.scenario << "," << res.query_type << "," << res.latency_microseconds << "\n";
    }
}

void run_mouse(DuckDB &db, std::string scenario_name, int delay_ms) {
    Connection con(db);
    std::vector<BenchResult> local_results;

    // Official TPC-H mice queries: Q6, Q14, Q22 — selective, fast, low resource usage
    static const std::string mouse_queries[] = {
        // TPC-H Q6: single lineitem scan, tight filters — fastest TPC-H query
        R"(SELECT sum(l_extendedprice * l_discount) AS revenue
           FROM lineitem
           WHERE l_shipdate >= CAST('1994-01-01' AS date)
             AND l_shipdate < CAST('1995-01-01' AS date)
             AND l_discount BETWEEN 0.05 AND 0.07
             AND l_quantity < 24;)",

        // TPC-H Q14: lineitem-part join, narrow 1-month date window
        R"(SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%'
               THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
               / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
           FROM lineitem, part
           WHERE l_partkey = p_partkey
             AND l_shipdate >= CAST('1995-09-01' AS date)
             AND l_shipdate < CAST('1995-10-01' AS date);)",

        // TPC-H Q22: customer table only, small aggregation
        R"(SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal
           FROM (SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal
                 FROM customer
                 WHERE substring(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17')
                   AND c_acctbal > (SELECT avg(c_acctbal) FROM customer
                                    WHERE c_acctbal > 0.00
                                      AND substring(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17'))
                   AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale
           GROUP BY cntrycode ORDER BY cntrycode;)"
    };

    int query_idx = 0;
    while (keep_running) {
        // Rotate through Q6, Q14, Q22
        std::string mouse_query = mouse_queries[query_idx % 3];
        query_idx++;

        auto start = high_resolution_clock::now();
        con.Query(mouse_query);
        auto end = high_resolution_clock::now();

        double duration = duration_cast<microseconds>(end - start).count();
        local_results.push_back({scenario_name, "Mouse", duration});

        // Simulate real-world arrival rates so we don't completely lock the CPU
        if (delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        }
    }

    std::lock_guard<std::mutex> lock(file_mutex);
    std::ofstream csv_file("mlfq_latency_results.csv", std::ios_base::app);
    for (const auto& res : local_results) {
        csv_file << res.scenario << "," << res.query_type << "," << res.latency_microseconds << "\n";
    }
}

void execute_scenario(DuckDB &db, std::string name, int duration_seconds, int num_elephants, int num_mice_threads, int mouse_delay_ms) {
    std::cout << "\n--- Starting Scenario: " << name << " ---" << std::endl;
    std::cout << "Duration: " << duration_seconds << "s | Elephants: " << num_elephants << " | Mice Threads: " << num_mice_threads << std::endl;
    
    keep_running = true;
    std::vector<std::thread> workers;

    // Spawn Elephants
    for(int i = 0; i < num_elephants; i++) {
        workers.push_back(std::thread(run_elephant, std::ref(db), name));
    }

    // Spawn Mice
    for(int i = 0; i < num_mice_threads; i++) {
        workers.push_back(std::thread(run_mouse, std::ref(db), name, mouse_delay_ms));
    }

    // Let the benchmark run for the specified duration
    std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));
    
    // Signal all threads to stop and wait for them to finish their current query
    std::cout << "Time up. Waiting for final queries to complete..." << std::endl;
    keep_running = false;
    
    for(auto& t : workers) {
        t.join();
    }
    std::cout << "Scenario " << name << " complete. Data appended to CSV." << std::endl;
}

int main() {
    // Initialize the CSV file with headers
    std::ofstream csv_file("mlfq_latency_results.csv");
    csv_file << "Scenario,QueryType,Latency_Microseconds\n";
    csv_file.close();

    DuckDB db("tpch-sf10.db");
    Connection setup_con(db);
    setup_con.Query("PRAGMA threads=4;");
    
    // Define runtime per scenario (300 seconds = 5 minutes)
    int RUN_TIME = 30; 

    // Scenario 1: Baseline Mice (No analytical load)
    // Proves the absolute best-case latency of your system
    execute_scenario(db, "Baseline_Mice_Only", RUN_TIME, 0, 10, 50);

    // Scenario 2: Mixed Workload
    // The main event. Proves MLFQ keeps Mice fast even when 2 Elephants are running.
    execute_scenario(db, "Mixed_Workload", RUN_TIME, 2, 10, 50);

    // Scenario 3: Heavy Contention (Optional Stress Test)
    // Saturates the system with Elephants to see if Q2 demotion prevents starvation
    execute_scenario(db, "Heavy_Contention", RUN_TIME, 8, 20, 10);

    std::cout << "\nAll benchmarks complete!" << std::endl;
    return 0;
}