// Copyright 2021 Research Institute of Systems Planning, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <memory>
#include <string>

#include "indicators/progress_spinner.hpp"
#include "caret_analyze_cpp_impl/progress.hpp"

Progress::~Progress()
{
  progress_->mark_as_completed();
}

void Progress::tick()
{
  progress_->tick();
}

Progress::Progress(std::size_t max_progress, std::string label)
{
  progress_ = std::make_shared<indicators::ProgressSpinner>();
  progress_->set_option(indicators::option::PostfixText{label});
  progress_->set_option(indicators::option::MaxProgress{max_progress});
  progress_->set_option(indicators::option::ShowElapsedTime{true});
  progress_->set_option(indicators::option::ShowRemainingTime{true});
}
