#include <iostream>
#include <fstream>
#include <chrono>
#include <list>
#include <unordered_map>

int main() {
    std::ifstream file;
    file.open("out", std::ios::in);
    if (!file.good()) {
      std::cout << "uhoh" << std::endl;
      return 1;
    }

    std::string line;
    std::list<std::pair<float, std::string>> batch;
    int i = 0;
    while (getline(file, line)) {
      batch.push_back({0, line});
      i++;
    }
    file.close();

    auto time_point = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    auto start = std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch());

    std::unordered_map<int, std::list<std::pair<float, std::pair<std::string, int>>>> keyed_counts;
    int num_reducers = 1;
    for (const auto &row : batch) {
      const auto &timestamp = row.first;
      const auto &line = row.second;
      size_t start = 0;
      size_t end = line.find(' ');
      while (end < line.size()) {
        auto word = line.substr(start, end - start);
        size_t h = std::hash<std::string>{}(word);
        h = h % num_reducers;
        keyed_counts[h].push_back({timestamp, {word, 1}});
        start = end + 1;
        end = line.find(' ', start);
      }
    }


    time_point = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
    auto end = std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch());
    std::cout << "lines: " << i << " took " << (end - start).count() << std::endl;

    return 0;
}
