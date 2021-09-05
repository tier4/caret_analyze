#include <memory>

#include "indicators/progress_spinner.hpp"
#include "trace_analysis_cpp_impl/progress.hpp"

Progress::~Progress()
{
  using namespace indicators;
  progress_->mark_as_completed();
}

void Progress::tick()
{
  progress_->tick();
}

Progress::Progress(std::size_t max_progress, std::string label)
{
  using namespace indicators;

  progress_ = std::make_shared<indicators::ProgressSpinner>();
  progress_->set_option(option::PostfixText{label});
  progress_->set_option(option::MaxProgress{max_progress});
  progress_->set_option(option::ShowElapsedTime{true});
  progress_->set_option(option::ShowRemainingTime{true});
}
