#include <memory>
#include <string>

#include "indicators/progress_spinner.hpp"

class Progress
{
public:
  Progress(std::size_t max_progress, std::string label = "");
  ~Progress();
  void tick();

private:
  std::shared_ptr<indicators::ProgressSpinner> progress_;
};
