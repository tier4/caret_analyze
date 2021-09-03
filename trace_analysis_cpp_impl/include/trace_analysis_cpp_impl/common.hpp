#include <unordered_set>

template<
  typename T,
  typename ContainerT = std::unordered_set<T>
>
ContainerT merge_set(const ContainerT & left, const ContainerT & right)
{
  ContainerT merged;
  for (auto & elem: left) {
    merged.insert(elem);
  }
  for (auto & elem: right) {
    merged.insert(elem);
  }
  return merged;
}
