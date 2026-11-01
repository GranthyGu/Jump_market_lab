#pragma once
// New demonstration infrastructure. No dependency on the retired test suite.
#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace demo {
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
inline void require(bool ok, const std::string& message) { if (!ok) throw std::runtime_error(message); }
inline double us(Clock::time_point start) { return std::chrono::duration<double,std::micro>(Clock::now()-start).count(); }
inline std::string json(const std::string& text) {
  std::ostringstream out; out << '"';
  for (unsigned char c : text) {
    if (c=='"' || c=='\\') out << '\\' << c;
    else if (c=='\n') out << "\\n";
    else if (c=='\r') out << "\\r";
    else if (c=='\t') out << "\\t";
    else if (c < 32) out << '?'; else out << c;
  }
  out << '"'; return out.str();
}
inline std::string read_text(const fs::path& path) {
  std::ifstream input(path); return {std::istreambuf_iterator<char>(input),{}};
}
template<class F> void eventually(F predicate, const std::string& message, int timeout_ms=8000) {
  auto end=Clock::now()+std::chrono::milliseconds(timeout_ms);
  do { if (predicate()) return; std::this_thread::sleep_for(10ms); } while (Clock::now()<end);
  throw std::runtime_error(message);
}
// Join before propagating an error; a worker failure never calls std::terminate.
template<class F> void parallel(unsigned count,F fn) {
  std::vector<std::thread> workers; std::mutex mutex; std::exception_ptr error;
  for (unsigned i=0;i<count;++i) workers.emplace_back([&,i] {
    try { fn(i); } catch (...) { std::lock_guard lock(mutex); if (!error) error=std::current_exception(); }
  });
  for (auto& worker:workers) worker.join();
  if (error) std::rethrow_exception(error);
}
struct Metric {
  std::string name,boundary; std::vector<double> values; double seconds; std::uint64_t operations;
};
class Suite {
  struct Case { std::string name,error; double milliseconds; };
  std::vector<Case> cases_; std::vector<Metric> metrics_;
 public:
  fs::path output;
  explicit Suite(fs::path directory):output(std::move(directory)) { fs::create_directories(output); }
  template<class F> void test(const std::string& name,F fn) {
    auto start=Clock::now(); std::string error;
    try { fn(); } catch (const std::exception& e) { error=e.what(); }
    cases_.push_back({name,error,us(start)/1000});
    std::cout << (error.empty()?"PASS ":"FAIL ") << name << " (" << cases_.back().milliseconds << " ms)";
    if (!error.empty()) std::cout << ": " << error;
    std::cout << std::endl;
  }
  void metric(std::string name,std::vector<double> values,double seconds,std::uint64_t operations,std::string boundary) {
    require(!values.empty() && seconds>0 && operations>0,"Empty performance measurement: "+name);
    metrics_.push_back({std::move(name),std::move(boundary),std::move(values),seconds,operations});
  }
  int finish() {
    unsigned failed=0; std::ofstream report(output/"suite.json"),raw(output/"samples.csv"),table(output/"metrics.csv");
    require(report.good() && raw.good() && table.good(),"Cannot create result files");
    raw << "metric,sample,latency_us\n";
    table << "metric,samples,operations,seconds,operations_per_second,mean_us,p50_us,p95_us,p99_us,max_us\n";
    report << "{\"cases\":[";
    for (std::size_t i=0;i<cases_.size();++i) {
      const auto& c=cases_[i]; if (!c.error.empty()) ++failed;
      report << (i?",":"") << "{\"name\":" << json(c.name) << ",\"passed\":" << (c.error.empty()?"true":"false")
             << ",\"duration_ms\":" << c.milliseconds << ",\"error\":" << json(c.error) << '}';
    }
    report << "],\"metrics\":[";
    for (std::size_t i=0;i<metrics_.size();++i) {
      const auto& m=metrics_[i];
      for (std::size_t j=0;j<m.values.size();++j) raw << m.name << ',' << j << ',' << std::setprecision(12) << m.values[j] << '\n';
      auto sorted=m.values; std::sort(sorted.begin(),sorted.end());
      auto q=[&](double p){return sorted[static_cast<std::size_t>(std::ceil(p*sorted.size()))-1];};
      const double mean=std::accumulate(sorted.begin(),sorted.end(),0.0)/sorted.size(),rate=m.operations/m.seconds;
      table << m.name << ',' << sorted.size() << ',' << m.operations << ',' << m.seconds << ',' << rate << ',' << mean
            << ',' << q(.5) << ',' << q(.95) << ',' << q(.99) << ',' << sorted.back() << '\n';
      report << (i?",":"") << "{\"name\":" << json(m.name) << ",\"samples\":" << sorted.size()
        << ",\"operations\":" << m.operations << ",\"seconds\":" << m.seconds << ",\"operations_per_second\":" << rate
        << ",\"mean_us\":" << mean << ",\"p50_us\":" << q(.5) << ",\"p95_us\":" << q(.95)
        << ",\"p99_us\":" << q(.99) << ",\"max_us\":" << sorted.back() << ",\"boundary\":" << json(m.boundary) << '}';
      std::cout << "METRIC " << m.name << " ops/s=" << rate << " p50_us=" << q(.5) << " p99_us=" << q(.99) << std::endl;
    }
    report << "],\"passed\":" << (failed?"false":"true") << ",\"failed\":" << failed << "}\n";
    std::cout << "RESULT " << (failed?"FAIL":"PASS") << " cases=" << cases_.size() << " failed=" << failed << std::endl;
    return failed?1:0;
  }
};
} // namespace demo
