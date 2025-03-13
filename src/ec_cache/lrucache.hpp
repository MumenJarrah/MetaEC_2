#ifndef _LRUCACHE_HPP_INCLUDED_
#define	_LRUCACHE_HPP_INCLUDED_

#include <unordered_map>
#include <list>
#include <cstddef>
#include <stdexcept>
#include <mutex>
#include <iostream>

using namespace std;

// namespace cache {

template<typename key_t, typename value_t>
class lru_cache {
public:
	typedef typename std::pair<key_t, value_t> key_value_pair_t;
	typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

	lru_cache(size_t max_size) :
		_max_size(max_size) {
			all_cnt = 0;
			miss_cnt = 0;
			miss_rate = 0.0;
	}
	
	void put(const key_t& key, const value_t& value) {
		std::lock_guard<std::mutex> lck(cache_mutex_);
		auto it = _cache_items_map.find(key);
		_cache_items_list.push_front(key_value_pair_t(key, value));
		if (it != _cache_items_map.end()) {
			_cache_items_list.erase(it->second);
			_cache_items_map.erase(it);
		}
		_cache_items_map[key] = _cache_items_list.begin();
		
		if (_cache_items_map.size() > _max_size) {
			auto last = _cache_items_list.end();
			last--;
			_cache_items_map.erase(last->first);
			_cache_items_list.pop_back();
		}
	}
	
	bool get(const key_t& key, value_t& value) {
		std::lock_guard<std::mutex> lck(cache_mutex_);
		auto it = _cache_items_map.find(key);
		// all_cnt ++;
		if (it == _cache_items_map.end()) {
			// throw std::range_error("There is no such key in cache");
			// cout << "There is no such key: [" << key << "] in cache" << endl;
			// miss_cnt ++;
			return false;
		} else {
			// cout << "hit: [" << key << "] in cache" << endl;
			_cache_items_list.splice(_cache_items_list.begin(), _cache_items_list, it->second);
			value = it->second->second;
			return true;
		}
	}

	std::list<key_value_pair_t> get_list(){
		std::lock_guard<std::mutex> lck(cache_mutex_);
		return _cache_items_list;
	}
	
	bool exists(const key_t& key) const {
		return _cache_items_map.find(key) != _cache_items_map.end();
	}
	
	size_t size() const {
		return _cache_items_map.size();
	}

	void remove(const key_t& key) {
		std::lock_guard<std::mutex> lck(cache_mutex_);
		auto it = _cache_items_map.find(key);
		if (it != _cache_items_map.end()) {
			_cache_items_list.erase(it->second);
			_cache_items_map.erase(it);
		}
	}

	double get_miss_rate(){
		return (double)miss_cnt / (double)all_cnt;
	}
	
private:
	std::list<key_value_pair_t> _cache_items_list;
	std::unordered_map<key_t, list_iterator_t> _cache_items_map;
	size_t _max_size;
	std::mutex cache_mutex_;

	size_t all_cnt;
	size_t miss_cnt;
	double miss_rate;
};

// } // namespace cache

#endif	/* _LRUCACHE_HPP_INCLUDED_ */

