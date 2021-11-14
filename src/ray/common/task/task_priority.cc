#include "ray/common/task/task_priority.h"

namespace ray {

void Priority::extend(uint64_t size) const {
  uint64_t diff = size -  static_cast<uint64_t>(score.size());
  if (diff > 0) {
    for (uint64_t i = 0; i < diff; i++) {
      score.push_back(INT_MAX);
    }
  }
}

void Priority::SetFromParentPriority(Priority &parent, int s){
  //param s id the last score to add
  if(parent.score.size() == 1 && parent.score[0] == INT_MAX){
	score[0] = s;
  }else{
	score = parent.score;
    score.push_back(s);
  }
}

bool Priority::operator<(const Priority &rhs) const {
  size_t r = rhs.score.size();
  size_t l = score.size();
  if(l<r){
	  for (uint64_t i = 0; i < l; i++) {
		  if(score[i]<rhs.score[i]){
			  return true;
		  }else if(score[i]>rhs.score[i]){
			  return false;
		  }
	  }
	  return true;
  }else if(l>r){
	  for (uint64_t i = 0; i < r; i++) {
		  if(score[i]<rhs.score[i]){
			  return true;
		  }else if(score[i]>rhs.score[i]){
			  return false;
		  }
	  }
	  return false;
  }
  return score < rhs.score;
}

bool Priority::operator<=(const Priority &rhs) const {
  size_t r = rhs.score.size();
  size_t l = score.size();
  if(l<r){
	  for (uint64_t i = 0; i < l; i++) {
		  if(score[i]<rhs.score[i]){
			  return true;
		  }else if(score[i]>rhs.score[i]){
			  return false;
		  }
	  }
	  return true;
  }else if(l>r){
	  for (uint64_t i = 0; i < r; i++) {
		  if(score[i]<rhs.score[i]){
			  return true;
		  }else if(score[i]>rhs.score[i]){
			  return false;
		  }
	  }
	  return false;
  }
  return score <= rhs.score;
}

bool Priority::operator>(const Priority &rhs) const {
  size_t r = rhs.score.size();
  size_t l = score.size();

  if(l<r){
	  for (uint64_t i = 0; i < l; i++) {
		  if(score[i]<rhs.score[i]){
			  return false;
		  }else if(score[i]>rhs.score[i]){
			  return true;
		  }
	  }
	  return false;
  }else if(l>r){
	  for (uint64_t i = 0; i < r; i++) {
		  if(score[i]<rhs.score[i]){
			  return false;
		  }else if(score[i]>rhs.score[i]){
			  return true;
		  }
	  }
	  return true;
  }
  return score > rhs.score;
}

bool Priority::operator>=(const Priority &rhs) const {
  size_t r = rhs.score.size();
  size_t l = score.size();
  if(l<r){
	  for (uint64_t i = 0; i < l; i++) {
		  if(score[i]<rhs.score[i]){
			  return false;
		  }else if(score[i]>rhs.score[i]){
			  return true;
		  }
	  }
	  return false;
  }else if(l>r){
	  for (uint64_t i = 0; i < r; i++) {
		  if(score[i]<rhs.score[i]){
			  return false;
		  }else if(score[i]>rhs.score[i]){
			  return true;
		  }
	  }
	  return true;
  }
  return score >= rhs.score;
}

std::ostream &operator<<(std::ostream &os, const Priority &p) {
  os << "[ ";
  for (const auto &i : p.score) {
    os << i << " ";
  }
  os << "]";
  return os;
}

std::ostream &operator<<(std::ostream &os, const TaskKey &k) {
  return os << k.second << " " << k.first;
}

}  // namespace ray
