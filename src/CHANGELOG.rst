^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Changelog for package caret_analyze
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

0.5.0 (2024-01-30)
------------------
* Update: package.xml version to 0.4.24 (`#471 <https://github.com/tier4/caret_analyze/issues/471>`_)
* fix: plot API annotations (`#463 <https://github.com/tier4/caret_analyze/issues/463>`_)
  * diff_node_names
  * 20230130
  * diff_node_names 20230205
  * re
  * class Diff
  * 20230205
  * refactor(arcitecture), test(test_arcitecture) / Naming conventions were changed and mock tests were added and modified because of the points raised
  * a
  * conflict fix2
  * fix: pytest error
  * refactor: fix plot api annotations about Timeseriestypes
  * refactor: add annotations of publisher and subscription to plot API
  * refactor: fix pytest error
  * docs: fix docs about period and frequency api
  * refactor: add the annotations of Pub and Sub to peri and freq hist plot api
  ---------
  Co-authored-by: taro-yu <milktea1621@gmai.com>
* docs: fix doc string in `value_object` package (`#454 <https://github.com/tier4/caret_analyze/issues/454>`_)
  * Fixed doc_string in CallbackStructValue.
  * Fixed doc_string in CallbackValue.
  * Fixed doc_string in ExecutorStructValue.
  * Corrected the beginning of the sentence in doc_string to be capitalized.
  * Periods were added to doc_string.
  * Fixed according to doc_string rules.
  * Fixed mypy error.
  * docs: fill lacked doc string
  * docs: fill doc string of NodePathValue
  * docs: fill doc string of NodeValue
  * docs: dill doc string of NodeValueWithId
  * docs: fill doc string in NodeStructValue
  * fix: minor mistake
  * docs: fill doc string of DiffNode
  * docs: fix argument
  * Fixed doc_string in ExecutorType.
  * Fixed according to doc_string rules.
  * Spelling corrected..
  * Fixed underbar
  * docs: fill doc string of ServiceCallbackValue
  * docs: doc string of ServiceCallbackStructValue
  * docs: fill doc string of ServiceValue
  * docs: fill doc string of ServiceStructValue
  * Fixed initials and periods in doc_string.
  * Fixed doc_string in ExecutorValue.
  * Fixed return anotation.
  * Fixed return anotation.
  * docs: fill doc string of SubscriptionCallbackStructValue
  * docs: fix minor mistake in CallbackValue
  * docs: fill doc string of SubscriptionCallbackValue
  * Fixed spell.
  * Fixed spell.
  * docs: fix minor mistake
  * docs: fix minor mistake in CallbackValue
  * docs: fix type mistake
  * docs: fix minor mistake
  * docs: add __init_\_ doc string
  * docs: fix upper case lacking
  * docs: fix pep
  * docs: fix upper case
  * docs: fix spell
  * docs: fill doc string of SubscriptionValue
  * docs: fix return type
  * docs: add __init_\_ doc string
  * Changed doc_string Return to Get..
  * docs: fix return type
  * docs: fill doc string of SubscriptionStructValue
  * Fixed doc_string in InheritUniqueStamp.
  * Fixed doc_string in MessageContext.
  * Fixed doc_string in MessageContextType.
  * Fixed doc_string in PathValue.
  * Grammatical errors were corrected..
  * Corrected omissions.
  * Grammatical errors were corrected..
  * Fixed spell miss.
  * docs: fix doc string TimerValue
  * docs: fix doc string TimerStructValue
  * docs: fix doc string TimerCallbackValue
  * docs: fix doc string TimerCallbackStructValue
  * Fixed doc_string in PublisherStructValue.
  * Fixed spell miss.
  * docs: fix doc string of Tilde (WiP)
  * docs: fix doc string of VariablePassingValue
  * docs: fix doc string of VariablePassingStructValue
  * docs: fix upper case
  * docs: fix doc string of __init_\_ lack variable name
  * docs: fix variable name in doc string in __init\_\_
  * Fixed doc_string.
  * Fixed doc_string in revision_api_list_publishervalue.
  * Corrected wording.
  * docs: fix blank line
  * docs: fix period
  * docs: fix upper case
  * docs: fix period
  * Fixed doc_string in Qos.
  * docs: fix doc string of is_applicable_path
  * docs: fill doc string of UseLatestMessage
  * Fixed return anotation.
  * docs: fix doc string of CallbackStructValue
  * docs: fix upper case and period
  * docs: fix upper case and period
  * docs: fix upper case and period
  * docs: fix upper case and period
  * docs: doc string of __str\_\_
  * docs: doc string of CallbackType
  * docs: fix period
  * minor mistake in publisher
  ---------
  Co-authored-by: emb4 <yuki.emori@tier4.jp>
  Co-authored-by: emori-ctc <129708475+emori-ctc@users.noreply.github.com>
* Contributors: h-suzuki-isp, r.okamu, yu-taro-

0.4.24 (2024-01-05)
-------------------
* fix: erase unused functions (`#442 <https://github.com/tier4/caret_analyze/issues/442>`_)
  * fix: erase old implementation of response time histogram
  * fix: erase unused old histogam and timeseries in response_time
  ---------
* feat: improvement of judgement ros distribution (`#452 <https://github.com/tier4/caret_analyze/issues/452>`_)
* fix: proc time not converted to sim time (`#451 <https://github.com/tier4/caret_analyze/issues/451>`_)
  * latency does not convert sim_time
  * Response_time does not convert sim_time
  * To pass pytest
  * Fix for pytest
  * Fixed X axis to sim_time conversion in staced_bar's to_dataframe().
  * ci(pre-commit): autofix
  * Added xaxis_type to the to_stacked_bar_data() argument.
  * ci(pre-commit): autofix
  * Added xaxis_type to the to_stacked_bar_data() argument.
  * fixed argument error passed to _find_node_path (`#453 <https://github.com/tier4/caret_analyze/issues/453>`_)
  * pytest support
  ---------
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
  Co-authored-by: miyakoshi <92290821+miyakoshi-dev@users.noreply.github.com>
* Contributors: ISP akm, h-suzuki-isp, r.okamu

0.4.23 (2023-12-25)
-------------------
* fixed argument error passed to _find_node_path (`#453 <https://github.com/tier4/caret_analyze/issues/453>`_)
* fix: bokeh interger warning (`#447 <https://github.com/tier4/caret_analyze/issues/447>`_)
  * fix: bokeh interger warning in timeseries
  * fix: bokeh interger wanring in message_flow and callback_scheduling
  * fix: flake8
  * fix: round float to int
  * fix: meanless description
  * fix: erase doc string of erased variable
  ---------
* refactor: upgrade xml version (`#448 <https://github.com/tier4/caret_analyze/issues/448>`_)
* fix: revision doc_string (`#446 <https://github.com/tier4/caret_analyze/issues/446>`_)
  * Fixed doc_string.
  * delete space.
  * Fixed omissions in parameter correction.
  * Corrections have been made to the points pointed out in the review.
  ---------
* feat: change xml for buildfarm (`#443 <https://github.com/tier4/caret_analyze/issues/443>`_)
  * feat: change for buildfarm
  * fix: remove unnecessary dependencies
  ---------
* feat: mypy test with `--check-untyped-defs` option (`#441 <https://github.com/tier4/caret_analyze/issues/441>`_)
  * feat: check-untyped-defs to mypy
  * fix: type check decorator
  * fix: assartion for preventing None in records_provider_lttng
  * fix: erase unused function
  * ci(pre-commit): autofix
  ---------
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* Contributors: emori-ctc, h-suzuki-isp, miyakoshi, r.okamu

0.4.22 (2023-12-12)
-------------------
* fix: error when drawn empty communication (`#445 <https://github.com/tier4/caret_analyze/issues/445>`_)
  * fix: drawn empty dataframe
  * fix: spelling miss
  * feat: add NOTE comment
  * fix: refactor NOTE comment
  ---------
* feat: add sim time to plot APIs (only non-supported APIs) (`#433 <https://github.com/tier4/caret_analyze/issues/433>`_)
  * create_response_time_timeseries_plot() API for sim_time.
  * Add support sim_time to histogram APIs.
  * fix: unified graph captions. (`#410 <https://github.com/tier4/caret_analyze/issues/410>`_)
  * Unified graph captions.
  * Unified conditional branching of graph captions.
  * Fixed flake8 errors.
  * Point corrected.
  * Corrected string notation.
  * Corrected string notation.
  * Corrected the points pointed out.
  * Corrected caption.
  * Corrected caption.
  ---------
  * chore(histogram): display number for Y-axis instead of probability (`#430 <https://github.com/tier4/caret_analyze/issues/430>`_)
  * chore(histogram): display number instead of probability
  * fix: change hover label
  ---------
  * Add sim_time support to create_response_time_stacked_bar_plot() API
  * Histogram legends click policy is set to "hide"
  * Skip invalid records that are involved in sim_time conversion but are invalid.
  * ci(pre-commit): autofix
  * chore: spell miss coverter->converter
  * fixed pytest errors
  * ci(pre-commit): autofix
  * fixed pytest errors
  * fixed pytest errors
  * fixed pytest errors
  * fixed pytest errors
  * fixed pytest errors
  * ci(pre-commit): autofix
  * fixed pytest errors
  * fixed pytest errors
  * add test codes
  * Reflection of PR comments (Part 1)
  * spelling convert->round_convert
  * Supported removal of _convert_timeseries_records_to_sim_time().
  * ci(pre-commit): autofix
  * Add sim_time support to StackedBarPlot.to_dataframe.
  * Fixed pytest error.
  * Fixed pytest error.
  * Fixed pytest error.
  * Reflection of PR comments (Part 2)
  * Reflection of PR comments (Part 3)
  ---------
  Co-authored-by: emori-ctc <129708475+emori-ctc@users.noreply.github.com>
  Co-authored-by: iwatake <take.iwiw2222@gmail.com>
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* fix: duplicate address support (`#342 <https://github.com/tier4/caret_analyze/issues/342>`_)
  * duplicate address support
  * first review results reflected
  * delete comments for debugging
  * improved spell checking
  * improved spell checking again
  * review results reflected and test code enhancement
  * delete unknown word
  * delete unknown word again
  * test code review results reflected
  * revert test_event_collection
  * comment change
  * delete unnecessary comments
* feat: support for no light arguremnt with topic_tools RelayNode in visualization (`#432 <https://github.com/tier4/caret_analyze/issues/432>`_)
  * add: delete null columns
  * add: create method for null check & get columns
  * update: additional description
  * update: add description
  * add: pytest for method of check_null & get_null_coumns
  * fix: to CRLF
  * Revert "fix: revert"
  This reverts commit bafb604ed8f1877797364f517f857d216f170808.
  * update: additional description
  * fix: spelling mistake
  fix: some spellings corrected
  fix: correct spelling errors
  fix: change wording
  fix: chage method name
  fix: supported by flake8
  * fix: 1 blank line required between summary line and description
  fix: 1 blank line between summary line and description
  * add: check_null & get_null_columns
  * fix: self explicitly passed as a parameter
  * fix: chage words
  * feat: changed to remove specific timestamps
  * Update src/caret_analyze/infra/lttng/records_provider_lttng.py
  Co-authored-by: ymski <yamasaki@isp.co.jp>
  * fix: change function name
  * fix: split one line into multiple lines
  * change function name & description
  * fix: remove functions and write them directly in the process
  * ci(pre-commit): autofix
  * fix: missing whitespace after ':'
  * feat: add generic test
  * fix: word literal
  * fix: style error
  * feat: add publisher test
  * fix: change comment message
  * Update src/caret_analyze/infra/lttng/records_provider_lttng.py
  Co-authored-by: ymski <yamasaki@isp.co.jp>
  ---------
  Co-authored-by: ymski <yamasaki@isp.co.jp>
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* Contributors: ISP akm, h-suzuki-isp, miyakoshi

0.4.21 (2023-11-27)
-------------------
* fix: support for mypy>1 (`#425 <https://github.com/tier4/caret_analyze/issues/425>`_)
  * chore: update requirement of mypy
  * fix: set default return value in message_context velify
  * fix: annotation of optional in graph_search
  * fix: annotation of optional in timeseries
  * fix: convert tuple to list
  * valify InheritUniqueStamp return False
  ---------
* chore: update maintainer (`#422 <https://github.com/tier4/caret_analyze/issues/422>`_)
  * chore: update maintainer
  * update setup.py
  ---------
* chore(histogram): display number for Y-axis instead of probability (`#430 <https://github.com/tier4/caret_analyze/issues/430>`_)
  * chore(histogram): display number instead of probability
  * fix: change hover label
  ---------
* Contributors: iwatake, r.okamu, ymski

0.4.20 (2023-11-13)
-------------------
* fix: unified graph captions. (`#410 <https://github.com/tier4/caret_analyze/issues/410>`_)
  * Unified graph captions.
  * Unified conditional branching of graph captions.
  * Fixed flake8 errors.
  * Point corrected.
  * Corrected string notation.
  * Corrected string notation.
  * Corrected the points pointed out.
  * Corrected caption.
  * Corrected caption.
  ---------
* feat: refactor best/worst record api in response time (`#323 <https://github.com/tier4/caret_analyze/issues/323>`_)
  * refact: best/worst records
  * fix: unnesessary discription
  * refact: worst (worst-to-best)
  * fix: flake8
  * fix: option name
  * fix: test
  * erase old tests (WiP)
  * fix: test to refactored functions
  * fix: order to start time
  * fix: pass pytest
  * feat: handling invalid response time
  ---------
* fix: support for bokeh 3.x (`#391 <https://github.com/tier4/caret_analyze/issues/391>`_)
  * fix: replace Figure to figure as Figure
  * fix: name of arg in bokeh newer version
  * fix: import for newer bokeh version
  * fix: invalid type of variable
  * fix: requirements bokeh version
  * fix: requirements bokeh version
  * fix: ignore bokeh calss type annotation
  ---------
* fix:  using caret_record_cpp_impl on GitHub Actions (`#405 <https://github.com/tier4/caret_analyze/issues/405>`_)
  * feat: use caret_record_cpp_impl
  * refactor: don't use record.py on GitHub Actions
  * style: consistent use of words
  style: consistent use of words
  * refoctor: Change the method of environment configuration
  * feat: output message In case of pytest failure
  * fix: changed message output method
  * feat: add command to explicitly indicate abnormal termination
  * Update .github/workflows/pytest.yaml
  Co-authored-by: isp-uetsuki <35490433+isp-uetsuki@users.noreply.github.com>
  ---------
  Co-authored-by: isp-uetsuki <35490433+isp-uetsuki@users.noreply.github.com>
* Contributors: emori-ctc, h-suzuki-isp, r.okamu

0.4.19 (2023-10-30)
-------------------
* fix: error of create_instance (`#403 <https://github.com/tier4/caret_analyze/issues/403>`_)
* fix: support for multimethod 1.10 (`#398 <https://github.com/tier4/caret_analyze/issues/398>`_)
  * fix: support for multimethod 1.10
  * fix: remove None
  ---------
* Contributors: Bo Peng, r.okamu

0.4.18 (2023-10-16)
-------------------
* chore/fix_repository_names (`#388 <https://github.com/tier4/caret_analyze/issues/388>`_)
* chore: upgrade pandas version (`#364 <https://github.com/tier4/caret_analyze/issues/364>`_)
* feat: support iron useful tracepoints (`#318 <https://github.com/tier4/caret_analyze/issues/318>`_)
  * changed to get or not get by ROS_Distribution
  * added publisher_handle information to rclcpp_publish from rcl_publish
  * refactor
  * change loading method
  * load ring_buffer tracepoint
  * impl: link method with ring_buffer
  * enable humble tracedata loading
  * pass flake8
  * pass  test
  * load buffer init trace points
  * impl check_ctf
  * impl iron init tp test
  * add test for check_ctf with iron-tp
  * fix dequeue_handler
  * load distribution info
  * validate using distribution info
  * add the case for no distributions in tracedata
  * add test for distribution
  * delete blank line
  * add distribution info to lttng_info
  * add test for lttng_info get_distribution()
  * remove comment
  * add intra_proc_com test for iron
  * refactor
  * Update src/caret_analyze/infra/lttng/ros2_tracing/data_model.py
  Co-authored-by: isp-uetsuki <35490433+isp-uetsuki@users.noreply.github.com>
  * Update src/caret_analyze/infra/lttng/records_source.py
  Co-authored-by: isp-uetsuki <35490433+isp-uetsuki@users.noreply.github.com>
  * Update src/caret_analyze/infra/lttng/records_source.py
  Co-authored-by: isp-uetsuki <35490433+isp-uetsuki@users.noreply.github.com>
  * Update src/caret_analyze/infra/lttng/records_source.py
  Co-authored-by: isp-uetsuki <35490433+isp-uetsuki@users.noreply.github.com>
  * remove unused property.
  * apply grouped by buffer
  * update iron test for intra proc
  * pass flake8
  * fix merge_record keywaord "how" for buffer enqueue/dequeue
  * Update src/caret_analyze/infra/lttng/records_source.py
  Co-authored-by: isp-uetsuki <35490433+isp-uetsuki@users.noreply.github.com>
  * update column name list
  ---------
  Co-authored-by: isp-uetsuki <35490433+isp-uetsuki@users.noreply.github.com>
* fix: specify setuptools version >= 68.2.2 (`#373 <https://github.com/tier4/caret_analyze/issues/373>`_)
* Contributors: Tetsuo Watanabe, takeshi-iwanari, ymski

0.4.17 (2023-10-03)
-------------------
* fix: setuptools newest version (`#372 <https://github.com/tier4/caret_analyze/issues/372>`_)
* fix:  rename option of ResponseTime (`#355 <https://github.com/tier4/caret_analyze/issues/355>`_)
  * fix: timeseries histogram option
  * fix: response time stacked bar option name
  * fix: default option
  * fix: doc string and naming
  * fix: syntax in doc string
  * fix: update description of response time
  * fix: minor mistake and flake8
  * fix: drop unvisualize column
  ---------
* fix: multimethod temporary fix (`#354 <https://github.com/tier4/caret_analyze/issues/354>`_)
* feat: add worst in input option to  response time timeseries (`#336 <https://github.com/tier4/caret_analyze/issues/336>`_)
  * diff_node_names
  * 20230130
  * diff_node_names 20230205
  * re
  * class Diff
  * 20230205
  * refactor(arcitecture), test(test_arcitecture) / Naming conventions were changed and mock tests were added and modified because of the points raised
  * a
  * conflict fix2
  * feat: add worst_in_input case option
  * refactor: change to worst-in-input
  * docs: change to worst-in-input
  * feat: add @staticmethod to response time timeseries plot
  * fix: pytest erorr
  * fix: pytest erorr
  ---------
  Co-authored-by: taro-yu <milktea1621@gmai.com>
* feat: response time histogram (`#349 <https://github.com/tier4/caret_analyze/issues/349>`_)
  * feat: response time histogram
  * feat: add doc string
  * fix: unpack for any class
  * fix: doc string type name
  * feat: case of response time histogram in title
  ---------
* remove progress_bar (`#332 <https://github.com/tier4/caret_analyze/issues/332>`_)
* Contributors: r.okamu, ymski, yu-taro-

0.4.16 (2023-09-21 10:11:22 +0900)
----------------------------------
* feat: add histogram api (`#317 <https://github.com/tier4/caret_analyze/issues/317>`_)
  * Added the ability to create histograms for callbacks.
  * The process of creating histograms has been moved to Bokeh.
  * update bokeh.py
  * Temporary push for source code verification
  * Push for code confirmation.
  * Push for code confirmation.
  * The error that occurred in pytest has been resolved.
  * Resolved pytest error.
  * The points raised in the review have been corrected.
  * rename variable
  * The points raised in the review have been corrected.
  * The points raised in the review have been corrected.
  * The points raised in the review have been corrected.
  * The points raised in the review have been corrected.
  * The graph legend has been corrected.
  * The vertical axis has been changed to a probability notation.
  * autoware is now supported.
  * Removed unnecessary comment-outs.
  * The pytest error has been resolved.
  ---------
* feat: stacked bar worst-in-input and all (`#339 <https://github.com/tier4/caret_analyze/issues/339>`_)
  * feat: stacked bar worst-in-input
  * feat: stacked bar all
  * fix: flake8
  * feat: test all stacked bar
  * feat: test worst-in-input stacked bar
  * fix: refactor
  * fix: spell
  * fix: pass mypy
  * fix: spell error
  ---------
* feat: response time timeseries (`#322 <https://github.com/tier4/caret_analyze/issues/322>`_)
  * diff_node_names
  * 20230130
  * diff_node_names 20230205
  * re
  * class Diff
  * 20230205
  * refactor(arcitecture), test(test_arcitecture) / Naming conventions were changed and mock tests were added and modified because of the points raised
  * a
  * conflict fix2
  * feat: make response time time series plot func
  * fix: conflict
  * fix: changed to use new record api
  * fix: changed to use new record api
  * fix: pytest errors
  * docs: add some description
  * docs: add some description
  * fix: argument error
  * feat: add hover info
  * docs: fix a comment
  * docs: added CARET_doc and fixed spell-ceheck-errors
  * docs: fixed spell-check-error
  * fix: mypy errors
  * fix: mypy errors
  * fix: mypy errors
  * fix: mypy errors and add some docs
  * fix: pytest errors
  * fix: mypy errors
  * docs remove some docs
  * feat: change hover info
  ---------
  Co-authored-by: taro-yu <milktea1621@gmai.com>
* fix(workaround): avoid the latest setuptools which causes build error (`#330 <https://github.com/tier4/caret_analyze/issues/330>`_)
* fix: Temporary handling of mypy warnings (`#328 <https://github.com/tier4/caret_analyze/issues/328>`_)
* Contributors: emori-ctc, r.okamu, takeshi-iwanari, yu-taro-

0.4.15 (2023-09-04)
-------------------
* fix: pandas version to avoid 2.1.0 (`#324 <https://github.com/tier4/caret_analyze/issues/324>`_)
* feat: response time worst in input (`#319 <https://github.com/tier4/caret_analyze/issues/319>`_)
  * feat: ResponseTime.to_worst_in_input_records
  * feat: test for worst in input
  * fix: typo
  * fix: invalid newline
  * fix: erase meaningless code
  ---------
* refactor: response time class api (`#313 <https://github.com/tier4/caret_analyze/issues/313>`_)
  * fix: rename _timeseries to _records
  * fix: erase to_records
  * fix: change to_timeseries type ndarray to RecordsInterface
  * fix: rename response records to stacked bar
  * docs: fix docstring
  * fix: typo
  * fix: comment
  * fix: comment
  * fix: warn old API
  * fix: warn old API
  * fix: flake8
  * fix: mypy
  * add: notation comments
  ---------
* fix warning for packages installed with apt (`#320 <https://github.com/tier4/caret_analyze/issues/320>`_)
* feat: duplicated callback id forced parsing (`#314 <https://github.com/tier4/caret_analyze/issues/314>`_)
  * feat:duplicated callback id forced parsing
  * reflection of review results
  * Revert "feat:duplicated callback id forced parsing"
  This reverts commit bef6e4458802b7594d133ea30e80e60dca71a8ed.
  * Revert leaked commit
  * changed to remove duplicates in lower layers
  * unknown word (reusal)
  * merge from pr312 for pytest
* feat: response time all (`#310 <https://github.com/tier4/caret_analyze/issues/310>`_)
  * feat: response time to_all_records
  * fix: test
  * feat: multi input single output test
  * fix: acccept drop case
  * fix: mistake of calcuration of drop case
  * fix: test
  ---------
* Contributors: miyakoshi, r.okamu, takeshi-iwanari, ymski

0.4.14 (2023-08-10)
-------------------
* fix: mypy warning in type_check_decorator by signiture (`#312 <https://github.com/tier4/caret_analyze/issues/312>`_)
  * fix: mypy warning
  * fix: type_check_decorator and those tests
  * fix: flake8
  * fix: expected type by annotation
  * fix: adopt error type
  * fix: doc string
  * fix: mypy
  * fix: mypy
  * fix: adapt union case in itrator
  * feat: comment for future works
  ---------
* feat: changes to the method of obtaining publisher_handle information (`#302 <https://github.com/tier4/caret_analyze/issues/302>`_)
  * changed to get or not get by ROS_Distribution
  * added publisher_handle information to rclcpp_publish from rcl_publish
  * refactor
  * refactor
  * change loading method
  * add publisher_handle info to beginnig_records
  * add label
  * add test for publisher_handle
  * pass flake8
  * refactor
  * added warning
  * added test for original rclcpp_publish
  * change warning message
  * add test for checking original-rclcpp
  * change message_timestamp in rclcpp_intra_publish to optional output content
  * change publisher_handle in rclcpp_publish to optional output content
  ---------
* Contributors: r.okamu, ymski

0.4.13 (2023-07-11)
-------------------

0.4.12 (2023-07-03)
-------------------
* fix: stack bar transparent and refactor stacked bar (`#307 <https://github.com/tier4/caret_analyze/issues/307>`_)
  * fix: erase uncommon discription
  * feat: staked bar without splited by func and class
  * refact: hover
  * add: StackedBarSource
  * feat: to_column_data_source
  * refact: hover to each bar
  * refact: erase unused codes and fix flake8, mypy
  * refact: scape of function and erase unused function
  * refact: improve readability
  * fix: hover
  * add: comment
  * fix: reverse legend
  * fix: flake8
  * fix: move function only in StackedBarSource
  * fix: fig.add_tools call once, update comment cleary
  * fix: remove description of adding 'label' and 'label' data to StackedBarSource
  * fix: improve readability
  * fix: improve readability
  * fix: use GraphRenderer.name
  * fix: remove _updated_timestamps_to_offset_time to StackedBarSource
  * fix: static method to member function
  * fix: reverse color
  ---------
* fix(plot): graph shifts when using the "xaxis=sim_time" option (`#306 <https://github.com/tier4/caret_analyze/issues/306>`_)
  * fix: typo
  * fix: typo
  * fix: symtax error (dict key acceess)
  * fix: round converted float to inst
  * add: notation comment
  * fix: convert function
  * ci(pre-commit): autofix
  * refactor: get column by converting records
  * fix: test of FrequencyTimeSeries._get_timestamp_range
  * fix: get columns records
  * fix: remove magic number: tid
  * fix: convert function of timeseries
  * fix: convert function of timeseries(`#276 <https://github.com/tier4/caret_analyze/issues/276>`_)
  * fix:conflict
  * fix: convert function for system_time to sim_time
  * fix: flake8
  * fix: mypy
  ---------
  Co-authored-by: rokamu623 <r.okamura.061@ms.saitama-u.ac.jp>
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* fix: check_ctf warning message (`#308 <https://github.com/tier4/caret_analyze/issues/308>`_)
  * fix warning message
  * add test
  * pass flake8
  * Update src/caret_analyze/infra/lttng/event_counter.py
  Co-authored-by: isp-uetsuki <35490433+isp-uetsuki@users.noreply.github.com>
  * minor change
  ---------
  Co-authored-by: isp-uetsuki <35490433+isp-uetsuki@users.noreply.github.com>
* Contributors: r.okamu, tamegaictc2, ymski

0.4.11 (2023-06-22)
-------------------
* docs: insert remove data path function (`#300 <https://github.com/tier4/caret_analyze/issues/300>`_)
  * fix: structure of pytests
  * add: definition of remove function
  * feat: remove_message_context init to UNDEFINED
  * ci(pre-commit): autofix
  * feat: remove_publisher
  * fix: flake8
  * feat: remove_passing provisional
  * fix: flake8 and mypy
  * add: docstring of assign\_ remove\_ func
  * add: docstring of remove\_ function
  * fix: erase unnessesary comment
  * fix: fix test
  * fix: delete remove_message_context and refactor assign_message_context
  * fix: remove_varialbe_passing fix to callback_chain
  * fix: typo
  * fix: naming of function
  * fix: redundant descriptions (is not None)
  * fix: erase unused funcion
  * fix: improve readability
  * fix: rename function in accordance with insert_xxx
  * fix: invalid indent
  * fix: flake8
  * fix: notation of None
  * fix: function name in pytest
  * fix insert_callback
  * fix: deal with chaing function name
  * fix: flake8
  * fix: review
  * fix: review
  * fix: review
  * fix: redundant
  * fix: review
  ---------
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* fix(plot): not working with "xaxis=sim_time" option (`#276 <https://github.com/tier4/caret_analyze/issues/276>`_)
  * fix: typo
  * fix: typo
  * fix: symtax error (dict key acceess)
  * fix: round converted float to inst
  * add: notation comment
  * fix: convert function
  * ci(pre-commit): autofix
  * refactor: get column by converting records
  * fix: test of FrequencyTimeSeries._get_timestamp_range
  * fix: get columns records
  * fix: remove magic number: tid
  * fix: convert function of timeseries
  * fix: convert function of timeseries(`#276 <https://github.com/tier4/caret_analyze/issues/276>`_)
  * fix:conflict
  * fix: erase Optional
  ---------
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
  Co-authored-by: tamegaictc2 <tsubasa.tamegai@tier4.jp>
  Co-authored-by: tamegaictc2 <129709232+tamegaictc2@users.noreply.github.com>
* fix: fix words to pass pytest (`#304 <https://github.com/tier4/caret_analyze/issues/304>`_)
* style: update type annotations (`#299 <https://github.com/tier4/caret_analyze/issues/299>`_)
  * fix: fix type hint description
  * fix: fix how to install pydantic
  * fix old description
  ---------
  Co-authored-by: atsushi421 <yff81986@nifty.com>
* Contributors: atsushi yano, r.okamu

0.4.10 (2023-06-09)
-------------------
* feat: remove function (`#297 <https://github.com/tier4/caret_analyze/issues/297>`_)
  * fix: structure of pytests
  * add: definition of remove function
  * feat: remove_message_context init to UNDEFINED
  * ci(pre-commit): autofix
  * feat: remove_publisher
  * fix: flake8
  * feat: remove_passing provisional
  * fix: flake8 and mypy
  * fix: erase unnessesary comment
  * fix: fix test
  * fix: delete remove_message_context and refactor assign_message_context
  * fix: remove_varialbe_passing fix to callback_chain
  * fix: typo
  * fix: naming of function
  * fix: redundant descriptions (is not None)
  * fix: erase unused funcion
  * fix: improve readability
  * fix: rename function in accordance with insert_xxx
  * fix: invalid indent
  * fix: flake8
  * fix: notation of None
  * fix: function name in pytest
  * fix insert_callback
  ---------
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* feat: support rmw take (`#296 <https://github.com/tier4/caret_analyze/issues/296>`_)
  * load rmw_take
  * fix: typo
  * pass flake8
  * impl: get sub_records via rmw_take tp
  * impl: inter_proc_comm_records via rmw_take
  * fix: fixed records linking method with rmw_take
  * impl: merge rmw_take and dispatch_subscription_callback records
  * impl: construct subscription_records from rmw_take and dispatch_subscription_callabck
  * refactor
  * test: remove message_timestamp
  * fix: remove message_stamp from docstring
  * refactor
  * chore: remove unused method
  * remove inter_proc_comm_records
  * remove message_timestamp from empty sub_records
  * add test for rmw_take
  * remove unnecessary column from rmw_sub_records
  * add condition to verify_communication
  * update tests to use rmw_take and refactor
  * delete duplicate tests
  * pass flake8
  * test: added rmw_take
  * fix typo
  * added rmw_take case
  ---------
* fix(plot): disable singledispatchmethod in plot_facade (`#294 <https://github.com/tier4/caret_analyze/issues/294>`_)
  * fix(plot): disable singledispatchmethod in plot_facade
  * docs: add docstring for create_response_time_stacked_bar_plot
  * docs: fix docstrings in plot_facade.py
  ---------
  Co-authored-by: atsushi421 <yff81986@nifty.com>
* feat(plot): draw callback latency at fitst and last in chain_latency (`#293 <https://github.com/tier4/caret_analyze/issues/293>`_)
  * feat(plot): draw callback latency at fitst and last in chain_latency
  * refactor: improve readability
  ---------
  Co-authored-by: atsushi421 <yff81986@nifty.com>
* Contributors: atsushi yano, r.okamu, ymski

0.4.9 (2023-05-12)
------------------
* fix(plot): fix to calculate period and frequency only when communication is established (`#291 <https://github.com/tier4/caret_analyze/issues/291>`_)
  * feat(record): add row_filter for Period class
  * fix(plot): fix to calculate period only when communication is established
  * feat(record): add row_filter for Frequency class
  * fix(plot): fix to calculate frequency only when communication is established
  * fix(plot): fix bug when target_object is empty
  ---------
  Co-authored-by: atsushi421 <yff81986@nifty.com>
* refactor(plot): refactor response time histogram plot (`#289 <https://github.com/tier4/caret_analyze/issues/289>`_)
  * style(plot): rename ResponseTimePlot to ResponseTimeHistPlot
  * refactor(plot): add ResponseTimeHistPlotFactory class
  * refactor(plot): changed to inherit PlotBase class in ResponseTimeHistPlot
  * refactor(plot): migrage the drawing process to visualize_lib in ResponseTimeHistPlot
  * refactor(plot): fix to not use old colour_selector
  * refactor(plot): fix to use init_figure
  * style(plot): improve readability
  * refactor(plot): remove unused hisgram_interface
  * chore(plot): add future annotations
  * Update src/caret_analyze/plot/histogram/histogram.py
  Co-authored-by: r.okamu <48247817+rokamu623@users.noreply.github.com>
  * style: fix deprecated annotations
  * style: improve readability
  ---------
  Co-authored-by: atsushi421 <yff81986@nifty.com>
  Co-authored-by: r.okamu <48247817+rokamu623@users.noreply.github.com>
* feat: path analysis including identical symbol objects (`#261 <https://github.com/tier4/caret_analyze/issues/261>`_)
  * Path analysis including identical symbol objects
  * Dealing with missing parts such as runtime
  * Second PR review results reflected
  * Remove spell-check-differential
  * Reflection of the results of the third review
  * Fix omission of get_message_contexts
  * 4th review results reflected
  * Reflection of test code review results
  * Reflection of test code review results
  * test_get_sub_pub Correction Mistake Correction
* chore(plot): remove node_graph and callback_graph (`#290 <https://github.com/tier4/caret_analyze/issues/290>`_)
  Co-authored-by: atsushi421 <yff81986@nifty.com>
* Contributors: atsushi yano, miyakoshi

0.4.8 (2023-04-27)
------------------
* fix(plot): fix legend for stacked bar plot (`#287 <https://github.com/tier4/caret_analyze/issues/287>`_)
  * fix_legend_for_stacked_bar_plot
  * style(plot): improve readability
  * fix(plot): recalculate latency to correct value
  * style(plot): improve readability
  * fix(plot): fix how to determine that it is the bottom of the stack
  ---------
  Co-authored-by: atsushi421 <yff81986@nifty.com>
* remove tid from callback record (`#285 <https://github.com/tier4/caret_analyze/issues/285>`_)
* refactor(plot): improve implementation for message flow (`#282 <https://github.com/tier4/caret_analyze/issues/282>`_)
  * chore(plot): remove old message_flow() API
  * refactor(plot): fix to specify legend_label as additional_hover_dict in HoverSource
  * refactor(plot): move get_callback_param_desc into util.py
  * refactor(plot): remove get_non_property_data function in HoverSource
  * refactor(plot): fix to use HoverSource in message flow
  * chore(plot) change the order of definitions in message_flow
  * docs(plot) improve readability for message_flow
  * Update src/caret_analyze/plot/visualize_lib/bokeh/message_flow.py
  Co-authored-by: r.okamu <48247817+rokamu623@users.noreply.github.com>
  * Update src/caret_analyze/plot/visualize_lib/bokeh/message_flow.py
  Co-authored-by: r.okamu <48247817+rokamu623@users.noreply.github.com>
  * fix(plot): rename y_mins to y_min_list
  ---------
  Co-authored-by: atsushi421 <yff81986@nifty.com>
  Co-authored-by: r.okamu <48247817+rokamu623@users.noreply.github.com>
* fix: minor changes for records processing (`#284 <https://github.com/tier4/caret_analyze/issues/284>`_)
* Contributors: atsushi yano, ymski

0.4.7 (2023-04-14)
------------------
* docs(plot): add docstring for plot package (`#280 <https://github.com/tier4/caret_analyze/issues/280>`_)
  * docs(plot): add docstring for create_message_flow_plot
  * docs(plot): add docstring for PlotBase class
  ---------
* refactor(plot): separate Bokeh class functions into classes (`#279 <https://github.com/tier4/caret_analyze/issues/279>`_)
  * chore(plot): rename message_flow_source to message_flow
  * refactor(plot): delegate message_flow()in Bokeh to BokehMessageFlow class
  * chore(plot): rename timeseries_source to timeseries
  * refactor(plot): delegate timeseries()in Bokeh to BokehTimeSeries class
  * chore(plot): rename stacked_bar_source to stacked_bar
  * refactor(plot): delegate stacked_bar()in Bokeh to BokehStackedBar class
  * chore(plot): rename callback_scheduling_source to callback_scheduling
  * refactor(plot): delegate callback_scheduling()in Bokeh to BokehCallbackSched class
  ---------
* refactor(plot): remove export_path argument and set return value to None in show() (`#278 <https://github.com/tier4/caret_analyze/issues/278>`_)
* refactor(plot): refactor hover in plot package (`#277 <https://github.com/tier4/caret_analyze/issues/277>`_)
  * refactor(plot): apply factory pattern to Hover-related classes.
  * refactor(plot): merge HoverCreator into HoverKeys
  * refactor(plot): add additional_hover_dict argument in HoverSource
  * chore(plot): fix typo
  * docs(plot): add docstring for Hover-related classes
  * chore(plot): fix typo
  * chore(plot): pass pytest
  * Update src/caret_analyze/plot/visualize_lib/bokeh/util/hover.py
  Co-authored-by: keita1523 <45618513+keita1523@users.noreply.github.com>
  * style(plot): add type hint
  * style(plot): fix type hint
  ---------
  Co-authored-by: keita1523 <45618513+keita1523@users.noreply.github.com>
* refactor(plot): improve directory structure in bokeh (`#275 <https://github.com/tier4/caret_analyze/issues/275>`_)
  * refactor(plot): move util.py into util directory
  * refactor(plot): move legend.py into util directory
  * refactor(plot): move color_selector.py into util directory
  * chore: pass flake8
  * refactor(plot): move common methods into util.py in Bokeh class
  * chore: add copyright
  * move hover-related classes to hover.py
  ---------
* fix(plot): enable_ywheel_zoom_by_default (`#274 <https://github.com/tier4/caret_analyze/issues/274>`_)
* Contributors: atsushi yano

0.4.6 (2023-04-03)
------------------
* fix(message_flow): include last callback to message flow with granularity='node' (`#273 <https://github.com/tier4/caret_analyze/issues/273>`_)
  * fix message_flow column format rule for include_last_callback
  * fixed branching conditions.
  ---------
* feat: stacked bar legend below (`#269 <https://github.com/tier4/caret_analyze/issues/269>`_)
  * diff_node_names
  * 20230130
  * diff_node_names 20230205
  * re
  * class Diff
  * 20230205
  * refactor(arcitecture), test(test_arcitecture) / Naming conventions were changed and mock tests were added and modified because of the points raised
  * a
  * conflict fix2
  * changed legend to below
  * ci(pre-commit): autofix
  * fix: add new fun for stacked-bar legends
  * fixed error to merge
  * ci(pre-commit): autofix
  * fixed pytest errors
  * fix: modified the new function and added docstring
  * fix: changed contents of create_legends() and deleted create_legends_bottom()
  * fix: fixed create_legends() to be more concise
  * fix: fixed the return value part of the docstring
  ---------
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* refactor(plot): refactor message flow (`#267 <https://github.com/tier4/caret_analyze/issues/267>`_)
  * refactor(plot): add template for message flow plot
  * refactor(plot): add message_flow interface in visualize_lib
  * refactor(plot): add validation for granularity
  * refactor(plot): migrate drawing process for message flow into visualize_lib
  * refactor(plot): add message flow hover
  * refactor(plot): add MessageFlowSource
  * refactor(plot): refactor to use MessageFlowSource
  * refactor(plot): refactor to use ColorSelector in message flow
  * refactor(plot): refactor not to use plot/util.py
  * refactor(plot): fix hover descriptions for callback rect
  * refactor(plot): rename message_flow.py into message_flow_old.py
  * refactor(plot): fix to call new interface from old interface
  * refactor(plot): remove unused plot/util.py
  * refactor(plot): improve readability
  ---------
* fix: include last callback to stacked bar graph (`#272 <https://github.com/tier4/caret_analyze/issues/272>`_)
  * fix: include last callback to stacked bar graph
  * test: add test case for callback_end in stacked_bar
  * chore: modify test code
  ---------
  Co-authored-by: keita1523 <keita.miura@tier4.jp>
* fix: callback name style of the first callback (`#271 <https://github.com/tier4/caret_analyze/issues/271>`_)
* feat(Path): extend path definition (`#263 <https://github.com/tier4/caret_analyze/issues/263>`_)
  * add tid to callback_start record
  * added beginning-point latency to Path definition (need refactoring)
  * added node_name to the beginning of the path
  * renamed function to_pub_partial_records
  * implemented extend path definition for message flow
  * implemented extend api
  * add tid column to callback_end_instances
  * drop tid from callback_start, callback_end
  * pass flake8
  * remove clone from callback_records
  * add docstring
  * pass flake8
  * absorb differences in implementation between Records and RecordsCppImpl
  * fixed test_var_pass
  * reset unnecessary change
  * remove path_end_records from records_provider_lttng
  * remove path_end_records from FilteredRecordsSource
  * fix docstring
  * remove path_end_records test
  * changed method name to avoid ambiguity
  * refactor path
  * include intra_process records to biginning path
  * pass flake8
  * refactor path.column_name
  * remove path_end_recorsd form RecordsSource
  * fixed annotation
  * added exception handling.
  * added testcode
  * remove compose_path_end_records
  * fix typo
  * rename enable_xxx to include_xxx
  * rename enable_xxx to include_xxx
  * pass flake8
  * rename enable_xxx to include_xxx in test code
  * refactor
  * fix docstring
  * enhanced testing of caches
  * pass flake8
  ---------
* feat(Architecture): add methods to get difference of two architecture object (`#245 <https://github.com/tier4/caret_analyze/issues/245>`_)
  * diff_node_names
  * 20230130
  * diff_node_names 20230205
  * re
  * class Diff
  * 20230205
  * refactor(arcitecture), test(test_arcitecture) / Naming conventions were changed and mock tests were added and modified because of the points raised
  * feat/ delete @staticmethod described in class Diff
  * 20230220
  * 20230220-5
  * a
  * spell miss fixed
  * pytesst error fixed
  * feat/test, add diff_node_topics/pubs/subs/callbacks, and tests for these func. Make DiffArchitecture and DiffNode class
  * refactor: Revisions were made to the review. The main modification was made to the naming.
  * docs: add doc string for the five diff functions.
  * refactor: change TestDiff to TestArchitectureDiff, and move Diffnode class in architecture.py to node.py
  * fix: remove the diff_callbacks func and the test codes about it
  * docs: added TODO comment in node.py
  * chore(caret_analyze): add comments for explaining diff class.
  * fix: remove pytest error
  * docs: added a statement on Returns, and brief descriptions
  ---------
  Co-authored-by: Takayuki AKAMINE <takayuki.akamine@tier4.jp>
* chore(plot): remove deprecated interface (`#266 <https://github.com/tier4/caret_analyze/issues/266>`_)
  * chore(plot): remove deprecated callback_sched function
  * chore(plot): remove deprecated Plot API
  ---------
  Co-authored-by: atsushi421 <a.yano.578@ms.saitama-u.ac.jp>
* feat: bokeh and plot packages for stacked bar (`#265 <https://github.com/tier4/caret_analyze/issues/265>`_)
  * feat: bokeh and plot packages for stacked bar
  * chore: typo
  * Apply suggestions from code review
  Co-authored-by: r.okamu <48247817+rokamu623@users.noreply.github.com>
  * chore: review
  * adjust color
  * chore: review
  * chore: review
  * chore: review
  * chore: apply pytest
  * chore: modify range
  * chore: pytest
  * fix: source size
  * fix: x_axis
  * feat: add path name in graph name
  * chore: delete print
  ---------
  Co-authored-by: r.okamu <48247817+rokamu623@users.noreply.github.com>
* feat: record for stacked bar (`#259 <https://github.com/tier4/caret_analyze/issues/259>`_)
  * feat: add stacked bar records
  * feat: add stacked bar core
  * feat: add test code
  * ci(pre-commit): autofix
  * chore: apply pytest
  * chore: typo
  * chore: apply to pytest
  * chore: typo
  * Update src/caret_analyze/record/records_service/response_time.py
  Co-authored-by: r.okamu <48247817+rokamu623@users.noreply.github.com>
  * chore: appy review
  * feat: add best case response records
  * chore: pytest
  ---------
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
  Co-authored-by: r.okamu <48247817+rokamu623@users.noreply.github.com>
* Contributors: atsushi yano, keita1523, takeshi-iwanari, ymski, yu-taro-

0.4.5 (2023-03-15)
------------------
* refactor: remove experiment package (`#264 <https://github.com/tier4/caret_analyze/issues/264>`_)
* chore: rename class name (`#260 <https://github.com/tier4/caret_analyze/issues/260>`_)
  * chore: rename class name
  * chore: pytest
* fix(plot): add unit into bokeh figure (`#256 <https://github.com/tier4/caret_analyze/issues/256>`_)
  * fix(plot): add unit into timeseries figure
  * refactor(plot): integrate duplicate processes into _init_figure()
  * fix(plot): align the size of bokeh figure
  * rename p to fig
  * chore(plot): rename yaxis_label to y_axis_label
  * refactor(plot): remove unnecessary dict
  ---------
  Co-authored-by: atsushi421 <a.yano.578@ms.saitama-u.ac.jp>
* chore(plot): split bokeh_source.py into multiple files (`#257 <https://github.com/tier4/caret_analyze/issues/257>`_)
  * chore(plot): move LineSource class into timeseries_source.py
  * chore(plot): move RectValues class into util.py
  * chore(plot): move CallbackSchedRectSource and CallbackSchedBarSource into callback_scheduling_source.py
  * chore(plot): rename bokeh_source.py into legend.py
  ---------
  Co-authored-by: atsushi421 <a.yano.578@ms.saitama-u.ac.jp>
* refactor(plot): improve readability of _apply_x_axis_offset() (`#258 <https://github.com/tier4/caret_analyze/issues/258>`_)
  Co-authored-by: atsushi421 <a.yano.578@ms.saitama-u.ac.jp>
* Contributors: atsushi yano, keita1523, takeshi-iwanari

0.4.4 (2023-02-20)
------------------
* refactor(plot): move Bokeh.get_range() to record package (`#254 <https://github.com/tier4/caret_analyze/issues/254>`_)
  * tests(plot): add tests for Range class
  * feat(plot): add Range class in record
  * refactor(plot): replace old get_range() with Range class
  * docs(record): add docstring into Range class
  * chore(plot): remove unnecessary mocker
  * tests(plot): add warning check
  * docs(plot): make docstring more detailed
  * fix(plot): fix condition for deciding whether data is valid or not
  ---------
* docs: documentation of rename_XXX (`#253 <https://github.com/tier4/caret_analyze/issues/253>`_)
  * docs: documentation of rename_XXX
  * fix: typo
  * fix: description of index
  * fix: doc string minor fix
  ---------
* fix(plot): fix corrupted x-axis by using AdaptiveTicker, and display … (`#255 <https://github.com/tier4/caret_analyze/issues/255>`_)
  * fix(plot): fix corrupted x-axis by using AdaptiveTicker, and display datetime instead of UNIX time for zero point
  * fix: pytest error
  * add comment on how to convert x-axis values to hhmmss format
  ---------
* Contributors: atsushi yano, r.okamu, takeshi-iwanari

0.4.3 (2023-02-03)
------------------
* exclude caret/record topics (`#251 <https://github.com/tier4/caret_analyze/issues/251>`_)
* fix(architecture): annotation for list to avoid runtime error (`#252 <https://github.com/tier4/caret_analyze/issues/252>`_)
* fix: support for warning message when architecture reader violates (`#241 <https://github.com/tier4/caret_analyze/issues/241>`_)
  * Support for warning message when architecture_reader violates uniqueness constraint.
  * typo
  * Reflection of review results.
  * spell-check-differential
  * Reflection of review results again.
  * update ExecutorValuesLoaded
  * fixed warning message
* chore(lttng): enlarge interval of tqdm. (`#248 <https://github.com/tier4/caret_analyze/issues/248>`_)
  Default interval (0.1) make processing slow when executing check_ctf via ssh with pexpect.spawn
* Contributors: miyakoshi, takeshi-iwanari, ymski

0.4.2 (2023-01-24)
------------------
* chore(runtime): add value property for debugging (`#247 <https://github.com/tier4/caret_analyze/issues/247>`_)
  * chore: add value property for debugging
  * typo
* refactor(plot): refactor callback scheduling (`#240 <https://github.com/tier4/caret_analyze/issues/240>`_)
  * chore(plot): move bokeh.py into bokeh directory
  * chore(plot): move LegendManager into bokeh_source.py
  * chore(plot): move LineSource into bokeh_source.py
  * feat(plot): add ColorSelector into color_selector.py
  * feat(plot): feat(plot): add BokehSourceInterface & refactor LineSource
  * feat(plot): feat(plot): add CallbackSchedRectSource & CallbackSchedBarSource
  * feat(plot): add CallbackSchedulingPlot except for Bokeh
  * feat(plot): add callback_scheduling in Bokeh
  * chore(plot): remove old callback_sched
  * test(plot): add tests for CallbackSchedulingPlot
  * test(plot): remove duplicate line
  * refactor(plot): add LegendKeys class
  * refactor(plot): add HoverCreator class
  * refactor(plot): add LegendSource class
  * refactor(plot): change to not use BokehSourceInterface
  * refactor(plot): decrease coupling degree of LegendManager.draw_legends.
  * refactor(plot): change color_selector argument type to Optional
  * fix(plot): add validation in ColorSelectorFactory
  * feat(plot): fix to continue processing even if callback group is None in callback_scheduling
  * chore(plot): interface -> method in warning message
  * docs(plot): add docstring for bokeh_source
  * refactor(plot): get_description -> get_non_property_data
  * chore(plot): remove default values for abstract method arguments in PlotBase
  * chore(plot): change default of ywheel_zoom of figure() to False in CallbackSchedulingPlot
  * fix(plot): changed show() and save() to use default values of figure() in Plot
* fix: validation for unknown columns (`#246 <https://github.com/tier4/caret_analyze/issues/246>`_)
  * add tests
  * pass tests
  * refactor duplicated_columns
  * refactor unknown_columns
  * pass flake8
  * refactor modify columns in mege_records validation
  * modify internal functions into private
  * add tests for merge and merge_sequential_for_addr_track_validate
  * pass tests
  * refactor rename local variables
  * refactor for columns_set variable
* refactor(lttng_info): remove redundant member variables (`#244 <https://github.com/tier4/caret_analyze/issues/244>`_)
  * remove _timer_cb_cache_without_pub variable
  * remove _sub_cb_cache_without_pub variable
  * remove _srv_cb_cache_without_pub variable
  * remove _timer_cb_cache variable
  * remove _sub_cb_cache variable
  * remove _srv_cb_cache variable
  * remove _pub_cache variable
  * remove _cbg_cache variable
  * remove reduncant local functions
  * modify get_publishers_without_cb_bind to protected
* refactor(record): mv latency.py, period.py,  etc. into records_service directory. (`#243 <https://github.com/tier4/caret_analyze/issues/243>`_)
  * move services to records_service directory
  * fix import paths
  * add: __init_\_.py in records_service
* fix: remove excessive assert and correct identation of assert (`#237 <https://github.com/tier4/caret_analyze/issues/237>`_)
  * refactor: remove excessive assertion
  * fix: indentation of assert
  * pass test
  * pass mypy
  * Apply suggestions from code review
  Co-authored-by: Takayuki AKAMINE <38586589+takam5f2@users.noreply.github.com>
  * Update src/caret_analyze/infra/lttng/lttng.py
  Co-authored-by: Takayuki AKAMINE <38586589+takam5f2@users.noreply.github.com>
  Co-authored-by: Takayuki AKAMINE <38586589+takam5f2@users.noreply.github.com>
* feat: add construction order of instances to the callback identification (`#225 <https://github.com/tier4/caret_analyze/issues/225>`_)
  * feat: add construction_order
  * typo
  * pass flake8
  * delete temporal scripts
  * add maching arguments to find_one
  * pass tests
  * supress duplicated warnings
  * fix: construction_order becomes None
  * typo
  * address comment
* fix: suppress duplicated callback id warnings (`#238 <https://github.com/tier4/caret_analyze/issues/238>`_)
* fix: assertion for duplicated node handlers (`#236 <https://github.com/tier4/caret_analyze/issues/236>`_)
* refactor: remove unused codes (`#234 <https://github.com/tier4/caret_analyze/issues/234>`_)
* chore(plot): remove deprecated jitter plot (`#231 <https://github.com/tier4/caret_analyze/issues/231>`_)
* feat: combine_path (`#224 <https://github.com/tier4/caret_analyze/issues/224>`_)
  * feat: handover
  * feat: modify
  * feat: modify
  * feat: modify
  * statsh
  * feat: develop path combine
  * feat: apply conflict
  * chore: apply to flake8 and mypy
  * chore: apply to github action
  * chore: apply to spell check
  * chore: add test code
  * feat: refactoring
  * ci(pre-commit): autofix
  * feat: minor change
  * ci(pre-commit): autofix
  * feat: refactoring
  * feat: refactoring
  * feat: refactor
  * feat: separate file
  * feat: separate file
  * feat: refactoring
  * feat: delete comment out
  * feat: type error
  * feat: rename
  * feat: minor
  * feat: pep
  * Apply suggestions from code review
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * feat: review
  * feat: pytest
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
* docs(common): add docstrings (`#214 <https://github.com/tier4/caret_analyze/issues/214>`_)
  * docs(common): add docstrings
  * Apply suggestions from code review
  Co-authored-by: Takayuki AKAMINE <38586589+takam5f2@users.noreply.github.com>
  * modify docstring in ClockConverter
  * ignore mypy
  Co-authored-by: Takayuki AKAMINE <38586589+takam5f2@users.noreply.github.com>
* Contributors: atsushi yano, hsgwa, keita1523

0.4.1 (2022-12-26)
------------------
* fix(plot): fix ValueError in create_publish_subscription_frequency_plot (`#227 <https://github.com/tier4/caret_analyze/issues/227>`_)
  * fix: error when no valid measurement data
  * mod: remove_dropped=False
* feat: assign (message/publisher/passing) function (`#196 <https://github.com/tier4/caret_analyze/issues/196>`_)
  * feat: defining of function
  * feat: Archtecture::assign_hoge
  * feat: easiest test
  * feat: assign fun WIP
  * feat: type of index
  * feat: search Callback in assign function
  * feat: assign_message_context (WIP)
  * feat: assign message context WIP
  * feat: new node_path assign in assign_message_context
  * feat: assign_message_context WIP
  * fix: publish topic
  * feat: test by template text
  * fix: remove protion test
  * fix: rename_function compliant
  * fix: API
  * feat: test invalid assign
  * fix: flake8
  * bug: nodes changed
  * ci(pre-commit): autofix
  * fix: assign publisher WIP
  * fix: assign publisher WIP
  * fix: test
  * fix: assign_publisher
  * fix: assign_passing
  * feat: additional test of assign_publisher
  * fix: comment
  * fix: flake8 mypy
  * feat: assign_message_context
  * feat: assign_message_context(WIP)
  * feat: assign_message_context(WIP)
  * feat: assign_message_contexts (DONE) and those tests (WIP)
  * feat: invalid and duplicated test of assign_message_context
  * fix: spell mistake
  * fix: test of assign_message_context
  * fix: pass duplicated and refactoring
  * fix: flake8 mypy
  * feat: docstring of AssignContextReader
  * fix: spell mistake
  * fix: minor mistake
  * erase redundant code
  * fix: unused function to be comment
  * fix: API and var names
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* refactor(records): clean RecordFactory.create_instance (`#197 <https://github.com/tier4/caret_analyze/issues/197>`_)
  * refactor(records): clean RecordFactory.create_instance
  * pass tests
  * address comment
  * add assert for mypy
* fix: contains unknown publisher_handle columns. (`#210 <https://github.com/tier4/caret_analyze/issues/210>`_)
  * fix: contains unknown publisher_handle columns.
  * address comment
  * add line break
* chore: add log level configuration (`#220 <https://github.com/tier4/caret_analyze/issues/220>`_)
  * add logging config
  * fixed to not use root logger
  * added log level configuration
  * fixed to not use root logger-s
  * changed logging method 'warn' to 'warning'
  * add copyright
  * update yaml
  * update yaml
  * ci(pre-commit): autofix
  * changed to handle file objects using "with"
  * changed to handle file objects using "with"
  * fixed log_config
  * changed log level
  * updated config
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* display empty figure if there is no data (`#223 <https://github.com/tier4/caret_analyze/issues/223>`_)
* chore(plot): add legend label to tooltips (`#222 <https://github.com/tier4/caret_analyze/issues/222>`_)
  * chore(plot): add legend label to tooltips
  * address comment
* refactor(plot): refactor Plot package (`#170 <https://github.com/tier4/caret_analyze/issues/170>`_)
  * refactor(plot): add temporary implementation
  * refactor(plot): refactor timeseries plot
  * style(plot): improve naming of classes
  * fix(plot): fix visualization units to ms
  * fix(plot): pass mypy
  * fix(plot): pass pep257
  * fix(plot): improve readability
  * refactor: add metrics class
  * chore: add copyright
  * fix: rename PlotInterface to PlotBase
  * fix: fix show() to return Figure object
  * refactor: refactor bokeh.timeseries()
  * refactor: refactor LineSource.generate()
  * chore: remove MetricsBase from __init\_\_
  * feat: add top level column
  * chore: add old interface into plot_facade
  * chore: move type_check_decorator to TimeSeriesPlotFactory
  * chore: add histogram
  * docs: add docstring & comment
  * chore: apply plot to refactored_plot
  * chore: rename refactored_plot to plot
  * tests: pass pytest in GithubAction
  * tests(plot): add test_metrics_base
  * tests(plot): add test_add_top_level_column
  * docs(plot): add docstring
  * fix(plot): add export_path option into show method
  * tests(plot): add test for _get_timestamp_range()
  * fix(plot): fit sim_time convertion method
  * fix(plot): change to property access for communication
  * fix(plot): change DataFrame to Records in _get_timestamp_range
  * fix(plot): fix to not output error in _get_timestamp_range
  * fix(plot): remove duplicated implementation
  * fix(plot): change to use logger
  * fix(plot): add check for length of records
  * fix(plot): remove TODO comments in graphviz
  * fix(plot): fix index in _get_timestamp_range
  * fix(plot): fix to not return an empty graph in frequency_timeseries
  * tests(plot): comment out tests related empty case
* Contributors: atsushi yano, hsgwa, r.okamu, ymski

0.4.0 (2022-12-16)
------------------
* feat(lttng): support for runtime recording of initialization trace points (`#190 <https://github.com/tier4/caret_analyze/issues/190>`_)
  * support ros2_caret:rcl_timer_init for runtime recording
  * fix: index error
  * pass flake8
  * remove tracetools version
  * support Initialization-related trace points for runtime recording
  * pass test
  * Changed to get the offset time first.
  * Add workaround implementation to exclude caret_trace node
  * Minor modifications. Deletion of useless codes, etc.
  * pass pytest
  * fix: assertion fail with monotonic_to_system_offset
  * fix: KeyError in is_ignored_subscription
  * fix: duplicated record
  * fix: wrong test case
  * fixed to catch exceptions to warn issues
  * fix: incorrect intra-proc comm records
  * pass flake8
  * typo
  * fix duplicates
  * fix: incorrect first data.
  * Merge branch 'main' into feat_runtime_record
* Contributors: hsgwa

0.3.4 (2022-12-13)
------------------
* fix(architecture_loaded): bug that overwrites name when name is non-None (`#198 <https://github.com/tier4/caret_analyze/issues/198>`_)
  * fix(architecture_loaded): bug that overwrites name when name is non-None
  * pass flake8
  * remove unnecessary comment
  * pass flake8
  * add TODO comments
* chore: rename test_path base.py to test_path_base.py (`#208 <https://github.com/tier4/caret_analyze/issues/208>`_)
* feat(architecture): suppress warnings (`#192 <https://github.com/tier4/caret_analyze/issues/192>`_)
  * add service-related callback class
  * add service_name to CallbackStruct
  * impl service value object
  * impl service struct
  * add service-related struct to __init\_\_
  * add service-related class to __init\_\_
  * add ServiceCallbackValueLttng
  * add service to interface
  * add service getter
  * add some attributes to service_df
  * impl get_service_calbacks
  * modify CallbacksLoaded to include service callback
  * Impl get_services for changes in base class
  * remove callback_object_intra from srv_df
  * eq and hash are not necessary because super method is used
  * ci(pre-commit): autofix
  * fix typo
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * fix typo
  * add test_get_service_callbacks
  * add service to test_get_callback_group_info
  * impl get_services, get_service_callbacks
  * add test for service
  * add test code for service
  * Changing the import order
  * pass flake8
  * add service_loaded
  * add service to NodeLoaded
  * change processing for subscription to service
  * change callback_type
  * impl service property and add test
  * pass flake8
  * remove callback_object_intra from service
  * remove japanese
  * rename services_df to service_callbacks_df
  * fix test_callback_name
  * add test_get_service_callbacks
  * add test_build_service_callbacks_df
  * add test_get_service_callbacks_info
  * pass flake8
  * fix conflict
  * Update src/caret_analyze/architecture/architecture_loaded.py
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * Update src/test/architecture/test_architecture_loader.py
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * add test for service
  * add test for service
  * fix doc string
  * Update src/caret_analyze/architecture/architecture_loaded.py
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * fix mypy
  * fix merge
  * fix: UnsupportedTypeError
  * remove unnecessary conditional statements
  * refactor: change instance verification method
  * change the naming convention of service_callback
  * add comment
  * Remove services from export file
  * fix flake8
  * fix pytest
  * fix pytest
  * fix pytest
  * fix flake8
  * fix flake8
  * remove japanese comments
  * fix typo
  * refactor
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
* feat: rename function (`#156 <https://github.com/tier4/caret_analyze/issues/156>`_)
  * feat: define function name
  * feat: test of rename_node
  * feat: rename_node and test
  * fix: find_one to find_similar_one0
  * fix: delete unused if
  * feat: rename_executor
  * feat: rename_executor
  * feat: rename_callback
  * feat: rename_node()
  * feat: rename_topic
  * refact: NamedPathManager to PathStruct
  * feat: rename_path
  * fix: mypy warning
  * error: test rename function
  * fix: callback topic test
  * fix: valid path test
  * fix: test rename architecture text
  * fix: delete path manager
  * fix: devide tests
  * fix: XXXStruct and Architecture and Archiecture_loaded to be mutable
  * fix: detail test
  * fix: multi callback group in test architecture
  * feat: test rename callback in publisher/subscriber
  * fix: flake8
  * fix: mypy
  * fix: ExecutorValuesLoaded return value
  * fix: API of add_path
  * fix: mistake
  * feat: test renamed instance not found
  * fix: annotation return value
  * fix: annotation return value
  * fix: find node path in add_path
  * fix: test arch.XXXs === arch.XXXs WIP
  * fix: test by object hash
  * fix: rename publish topic
  * fix: test compare all object
  * feat: test to template string
  * fix: stable order of object in architecture
  * fix: redundant and incorrect test
  * fix: redundant test
  * fix: mypy flake8 warning
  * erase unnesessary code
  * fix: flake8 warnning
  * fix: flake8 warnning
* chore: adapt mypy (`#211 <https://github.com/tier4/caret_analyze/issues/211>`_)
  * chore: adapt pep257
  * ci(pre-commit): autofix
  * chote: apply suggestions from code review
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * chore: apply review
  * ci(pre-commit): autofix
  * chore: apply review
  * ci(pre-commit): autofix
  * chore: apply mypy
  * chore: add assert to apply None
  * chore: delete skip github command
  * Update src/caret_analyze/plot/bokeh/callback_sched.py
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * Update src/caret_analyze/plot/bokeh/callback_sched.py
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * Update src/test/test_mypy.py
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * chore: apply to mypy and flake8
  * chore: modify code
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
* fix(intra): add sorting to communication.to_records and publisher.to_records (`#213 <https://github.com/tier4/caret_analyze/issues/213>`_)
  * fix(intra): add sorting to communication.to_records
  * add: comment
  * fix(intra): add sorting to publisher.to_records
* docs(exceptions,value_objects): add docstring (`#215 <https://github.com/tier4/caret_analyze/issues/215>`_)
  * docs(exceptions,value_objects): add docstring
  * address comment
* chore: fix singledispatchmethod to make docstring visible (`#207 <https://github.com/tier4/caret_analyze/issues/207>`_)
  * chore: add functools.wraps into singledispatchmethod
  * fix(plot): fix singledispatchmethod
  * fix: remove wraps from interface.py and trace_point_data.py
  * fix: remove wraps from protected functions
  * fix: pass pep257
* Contributors: atsushi yano, hsgwa, keita1523, r.okamu, ymski

0.3.3 (2022-11-23)
------------------
* feat: add dynamic type check decorator & use it to check Path input (`#186 <https://github.com/tier4/caret_analyze/issues/186>`_)
  * chore(plot): improve error message when PathStructValue inputted
  * feat: add type_check_docorartor
  * fix: remove type checking when PathStructValue inputted
  * feat: add type_check_decorator using pydantic
  * fix(plot): fix mypy error in chain_latency
  * fix(plot): change Collection to Sequence in histogram
  * fix: pass flake8
  * chore: add pydantic into package.xml
  * fix: fix ModuleNotFoundError to ImportError
  * chore: add copyright into type_check_decorator.py
  * style(common): improve error message in type_check_decorator
  * fix(common): add return in type_check_decorator
  * fix(common): kargs -> kwargs
  * test(common): add tests for type_check_decorator
  * test(common): pass flake8
  * style(common): improve readability of type_check_decorator
  * fix(common): InvalidArgumentError -> UnsupportedTypeError in type_check_decorator
  * fix(common): typo
  * fix(common): typo
  * tests: skip tests in github action
  * fix: pass flake8
  * fix: add functool.warp into type_check_decorator
  * feat: add given argument type in type_check_decorator
  * docs: improve docstring
  * fix: pass flake8 & pep257
  * fix: pass pep257
  * docs: add return value in docstring
  * tests: enable tests in github actions
  * style: improve readability
* fix(plot): bug in frequency plot start time is out of alignment (`#201 <https://github.com/tier4/caret_analyze/issues/201>`_)
  * fix(record): support until_timestamp argument for Frequency
  * fix(plot): changed to use of records.Frequency
  * pass flake8
  * rename function name to _get_timestamp_range
* chore: remove duplicated publishers and subscriptions (`#206 <https://github.com/tier4/caret_analyze/issues/206>`_)
  * chore: remove duplicated publishers and subscriptions
  * remove redundant comments
* fix(infra): disable multimethod to avoid recursion depth exceed (`#205 <https://github.com/tier4/caret_analyze/issues/205>`_)
* chore: adapt pep (`#204 <https://github.com/tier4/caret_analyze/issues/204>`_)
  * chore: adapt pep257
  * ci(pre-commit): autofix
  * chote: apply suggestions from code review
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * chore: apply review
  * ci(pre-commit): autofix
  * chore: apply review
  * ci(pre-commit): autofix
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
* chore: set max_node_depth default and add messages in search_paths (`#200 <https://github.com/tier4/caret_analyze/issues/200>`_)
  * chore: set max_node_depth default and add messages in search_paths
  * fix(architecture): improve message in search_paths
* chore: ignore wird flake8 error (`#199 <https://github.com/tier4/caret_analyze/issues/199>`_)
* refactor(lttng): add TracePointData class for pandas.Dataframe wrapper (`#193 <https://github.com/tier4/caret_analyze/issues/193>`_)
  * refactor(lttng): add TracePointData class for pandas.Dataframe wrapper
  * typo
  * pass pep257
  * fix: pd.NA check error. modify np.nan to pd.NA
  * pass flake8
  * fix: int() argument must be a real number, not 'NoneType'
* chore: support publisher-only / subscription-only topic names (`#180 <https://github.com/tier4/caret_analyze/issues/180>`_)
  * chore: add publishers and subscriptions
  * chore: add publishers and subscriptions to architecture
  * chore: add publisher-only or subscription-only to topic_name
  * pass flake8
* feat(lttng): automatic cache updating (`#189 <https://github.com/tier4/caret_analyze/issues/189>`_)
  * chore(pytest): add no:launch_testing for caplog test in humble
  * feat(lttng): check valid cache exists
  * typo
  * docs: add docstrings for exists function
  * address comment: path -> Path
* Contributors: atsushi yano, hsgwa, keita1523

0.3.2 (2022-11-08)
------------------
* fix: callbacks return value (`#188 <https://github.com/tier4/caret_analyze/issues/188>`_)
  * fix: callbacks return value
  * fix: erase redundant []
  * fix: minor mistake lacing {
* fix: fix warnings for pandas.groupby (`#187 <https://github.com/tier4/caret_analyze/issues/187>`_)
* feat: add callback symbol name to warning message in Lttng and Architecture (`#182 <https://github.com/tier4/caret_analyze/issues/182>`_)
  * tests(ros2_tracing): add callback symbol
  * tests(ros2_tracing): pass flake8
  * feat(ros2_tracing): modify API to get node name and callback symbol from callback group address
  * feat: fix get_node_names to get_node_names_and_cb_symbols
  * feat: add callback symbols into warning message
  * fix(ros2_tracing): support cases where multiple symbols correspond to one handle
* chore: add publishers and subscriptions properties (`#179 <https://github.com/tier4/caret_analyze/issues/179>`_)
  * chore: add publishers and subscriptions
  * chore: add publishers and subscriptions to architecture
* refactor(callback): specify the name of the keyword argument (`#185 <https://github.com/tier4/caret_analyze/issues/185>`_)
  * refactor
  * pass flake8
* chore: fix bokeh version 2.x (`#183 <https://github.com/tier4/caret_analyze/issues/183>`_)
* feat(lttng): add FileNotFound exception when file is not found (`#172 <https://github.com/tier4/caret_analyze/issues/172>`_)
  * feat(lttng): add exception when file is not found
  * apply comment
* feat: add api showing histogram of response time (`#165 <https://github.com/tier4/caret_analyze/issues/165>`_)
  * feat: add api showing histogram of response time
  * fix: output change to one plot
  * fix: add Copyright
  * fix: add Copyright and add the arguments
  * fix: default values
  * fix: support scientific notation
* Contributors: Bo Peng, atsushi yano, hsgwa, r.okamu, takeshi-iwanari, ymski

0.3.1 (2022-10-31)
------------------
* fix(lttng): pass dtype miss-match error (`#169 <https://github.com/tier4/caret_analyze/issues/169>`_)
* test: adapt humble flake8 (`#171 <https://github.com/tier4/caret_analyze/issues/171>`_)
  * test: adapt humble flake8
  * test: modify word choice
* fix: to_dataframe type error (`#157 <https://github.com/tier4/caret_analyze/issues/157>`_)
* feat(record): add API to get period time-series data (`#168 <https://github.com/tier4/caret_analyze/issues/168>`_)
  * feat(record): add API to get period time-series data
  * fix(record/period): remove unnecessary process
  * fix(record/period): fix left column to use former timestamp
  * fix(record/period): pass flake8
* feat: add node names in warning message when callback group not found (`#162 <https://github.com/tier4/caret_analyze/issues/162>`_)
  * feat: add node names in warning message when callback group not found
  * feat: add get_node_names into ArchitectureReaderYaml
  * feat(value_objects): add CallbackGroupAddr and CallbackGroupId
  * fix: fix conversion method between callback group address and id
  * docs: fix docstring
* feat(record): add API to get latency time-series data (`#167 <https://github.com/tier4/caret_analyze/issues/167>`_)
  * feat(record): add API to get latency time-series data
  * fix(record/latency): remove unnecessary process
* feat(record): add API to get frequency time-series data (`#116 <https://github.com/tier4/caret_analyze/issues/116>`_)
  * test: add tests for latency, period, and frequency
  * test(record): fix frequency test case and remove latency and period tests
  * test(record) fix argument passing
  * test(record): fix initial_timestamp to base_timestamp and improve readability
  * test(record): fix columns
  * refactor: add Frequency class into record
  * style(plot): improve readability
  * fix(plot): fix logic error
  * docs(record): add docstring in frequency
  * perf(record): change not to use records.get_column_series
  * style(record): rename function name
  * docs(record): fix docstring
  * tests(record): add one_interval_contains_all_timestamps_case
  * fix(record): address IndexError
  * fix(record): address KeyError in record.get()
  * fix(record): support for cases where zero frequency occurs
  * style(record): improve readability of frequency.py
  * fix(record/frequency): support for cases where base_timestamp is greater than minimum timestamp
  * style(record): improve readability of frequency.py
  * style(record/frequency): improve readability
  * fix(record/frequency): fix how to check whether timestamp exists or not
  * fix(record): fix test to confirm whether specified column was used
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
* fix(lttng): value error in pandas merge "trying to merge on object and float64 columns" (`#163 <https://github.com/tier4/caret_analyze/issues/163>`_)
* test: modify mypy-related environment (`#159 <https://github.com/tier4/caret_analyze/issues/159>`_)
  * add: mypy.ini
  * add: types-pyyaml
  * add args to use config file
  * add: eol
* refactor(plot): unify interface of time-series plot API to Collection[XXX] (`#142 <https://github.com/tier4/caret_analyze/issues/142>`_)
  * fix(plot): unify interface of time-series plot API
  * refactor(plot): allow unpack
  * fix(plot): fix mypy error
  * fix(plot): changed from passing class variables to passing arguments
* feat(lttng): add node name to warning message (`#158 <https://github.com/tier4/caret_analyze/issues/158>`_)
* fix: mypy errors in infra, runtime and value_objects (`#147 <https://github.com/tier4/caret_analyze/issues/147>`_)
  * fix: pandas.Dataframe error 'columns cannot be a set error'
  * fix: type annotations
  * add: type ignore to temporal variables
  * comment out unused codes
  * fix: wrong argument
  * pass flake8
  * add warnig when publisher is None
  * pass: tests
* chore: move LttngEventFilter to lttng_event_filter.py (`#146 <https://github.com/tier4/caret_analyze/issues/146>`_)
  * fix: pandas.Dataframe error 'columns cannot be a set error'
  * chore: move LttngEventFilter to lttng_event_filter.py
  * modify init to use as before
* feat: modify Architecute internal data to mutable (`#84 <https://github.com/tier4/caret_analyze/issues/84>`_)
  * feat_func_name
  * ci(pre-commit): autofix
  * feat_StructXXX
  * feat_StructXXX
  * feat_XXXStruct
  * ci(pre-commit): autofix
  * fix_import_error
  * fix_XXXtype_import
  * import
  * ci(pre-commit): autofix
  * remove_and_change_import_of_struct_package
  * ci(pre-commit): autofix
  * feat_interface_in_architecture
  * feat_define_to_value
  * feat_to_value
  * feat_fix_return_struct
  * ci(pre-commit): autofix
  * feat_message_context_struct
  * fix_optional
  * ci(pre-commit): autofix
  * fix_var_name
  * fix_return_struct
  * fix_tests
  * ci(pre-commit): autofix
  * fix_typo
  * fix_comments
  * fix_pathmgr
  * fix: flake8_error
  * fix: comment class notation
  * fix: return of path mgr
  * fix: type top -> to
  * fix: lacking to_value
  * fix: flake8 warning
  * fix: update copyright
  * feat: None ckeck
  * fix: mock test
  * fix: class anotation
  * fix: path searcher return class
  * fix: remove instantiation of abstract class
  * fix: optimal
  * fix: annotation
  * fix: check procedure return value
  * fix: test for search path
  * fix: coding rule
  * fix: delete unused function
  * fix: comment
  * fix: archtecture member class
  * fix: coding rule and class
  * fix: class of callback member
  * resolve conflict
  * fix: slight mistake
  * fix: compare with value
  * fix: fix optional
  * fix: delete unused text
  * fix: typo
  * fix: delete summary()
  * fix: remove summarizable
  * fix: fix None
  * fix: fix None by func
  * fix: minor mistake
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* feat(runtime): add callbacks property in Plot object (`#154 <https://github.com/tier4/caret_analyze/issues/154>`_)
  * chore(runtime): add callbacks property in Plot object
  * fix(runtime): pass mypy
* feat(ros2_tracing): add API to get node name from callback group address based on Ros2DataModel. (`#144 <https://github.com/tier4/caret_analyze/issues/144>`_)
  * tests(ros2_tracing): add DataModelService template and tests
  * tests(ros2_tracing): fix obtained node_name
  * tests(ros2_tracing): fix minor miss
  * feat(ros2_tracing): add API to get node_name from cbg_addr
  * fix(ros2_tracing): remove unnecessary print
  * fix(ros2tracing): support for duplicate address case
  * fix(ros2_tracing): fix type hints
  * fix(ros2_tracing): fix flake8
  * chore(ros2_tracing): add comments
  * fix(ros2_tracing): fix review comments
  * fix(ros2_tracing): pass flake8
  * fix(ros2_tracing): fix to not throw exception
  * Update src/caret_analyze/infra/lttng/ros2_tracing/data_model_service.py
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
* fix: message_flow assert with use_sim_time=True (`#153 <https://github.com/tier4/caret_analyze/issues/153>`_)
* chore(runtime): rename from callbacks to callback_chain in Path's property (`#145 <https://github.com/tier4/caret_analyze/issues/145>`_)
* docs: prettify api document (`#152 <https://github.com/tier4/caret_analyze/issues/152>`_)
* fix: typo in validation error message (`#151 <https://github.com/tier4/caret_analyze/issues/151>`_)
* Contributors: atsushi yano, hsgwa, keita1523, r.okamu, takeshi-iwanari

0.3.0 (2022-09-26)
------------------
* fix(lttng): bug with different range of duration_filter when there is no cache (`#148 <https://github.com/tier4/caret_analyze/issues/148>`_)
  * fix(lttng): bug with different range of duration_filter when there is no cache
  * typo
  * add comments
* chore(value_objects): add node name to summary dict (`#139 <https://github.com/tier4/caret_analyze/issues/139>`_)
* fix: pandas.Dataframe error 'columns cannot be a set error' (`#141 <https://github.com/tier4/caret_analyze/issues/141>`_)
* chore: change local variable names (`#135 <https://github.com/tier4/caret_analyze/issues/135>`_)
  * chore: change local variable names
  * fix(pytest): fix pytest error
  * chore: rename kargs->kwargs
* fix(plot): fix tooltip in pubsubTimeSeriesPlot.show() to node/topic name (`#140 <https://github.com/tier4/caret_analyze/issues/140>`_)
  * fix(plot): fix tooltip in pubsubTimeSeriesPlot.show() to node/topic name
  * style(plot): remove unneccesay comment
  * fix(plot): add processing when interactive option is enabled
  * Update src/caret_analyze/plot/bokeh/pub_sub_info_interface.py
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  * ci(pre-commit): autofix
  Co-authored-by: hsgwa <19860128+hsgwa@users.noreply.github.com>
  Co-authored-by: pre-commit-ci[bot] <66853113+pre-commit-ci[bot]@users.noreply.github.com>
* fix(plot): add error handling for to_dataframe() if size 0 (`#130 <https://github.com/tier4/caret_analyze/issues/130>`_)
  * fix(plot): add error handling for to_dataframe() if size 0
  * fix(plot): fix warning message
  * fix(plor): move size 0 error handling to for loop
  * fix(plot): change finally to else
  * fix(plot): to guarantee existence of source_df column
  * refactor(plot): remove redundant to_dataframe()
  * fix flake8
* chore(plot): comment out interactive option (`#134 <https://github.com/tier4/caret_analyze/issues/134>`_)
  * chore(plot): comment out interactive option
  * fix(plot): fix return value of PubSubTimeSeriesPlot to Figure
* fix: chrome sigill error (`#137 <https://github.com/tier4/caret_analyze/issues/137>`_)
* fix(plot): add handling of cases where there is no node/callback name (`#136 <https://github.com/tier4/caret_analyze/issues/136>`_)
  * fix(plot): add handling of cases where there is no node/callback name
  * tests(plot): fix test_pub_sub_info_interface
* fix(plot): fix todo comment in Plot (`#133 <https://github.com/tier4/caret_analyze/issues/133>`_)
  * fix(plot): fix todo comment
  * tests(plot): add tests for _get_ts_column_name()
* fix(sequential): typo error (`#128 <https://github.com/tier4/caret_analyze/issues/128>`_)
* docs(plot): add todo (`#131 <https://github.com/tier4/caret_analyze/issues/131>`_)
  * docs(plot): add todo memo
  * add todo about missing definition for self._communication
* chore(plot): change plot APIs to return figure object (`#124 <https://github.com/tier4/caret_analyze/issues/124>`_)
  * chore: change plot APIs to return figure object
  * fix(plot): fix if sentence of not interactive
  * fix(plot): fix not interactive processing
  * style(plot): fix all_topic_names
  * fix(plot): modify interactive default value as False
* fix: change plot APIs to return figure object (`#129 <https://github.com/tier4/caret_analyze/issues/129>`_)
  The original changes are PR `#109 <https://github.com/tier4/caret_analyze/issues/109>`_, but some changes were accidentally removed at PR `#108 <https://github.com/tier4/caret_analyze/issues/108>`_
* test(lttng): pass event_counter tests (`#123 <https://github.com/tier4/caret_analyze/issues/123>`_)
* fix(words): typo error (`#120 <https://github.com/tier4/caret_analyze/issues/120>`_)
  * fix(words): typo error
  * fix(words): typo error
  * fix(pytest): fix pytest error
  * fix(words): typo error
  * fix(words): typo error
  * fix(words): typo error
* fix(lttng): change caret-rclcpp violation as warn (`#121 <https://github.com/tier4/caret_analyze/issues/121>`_)
* fix(lttng): missing implementation of `#117 <https://github.com/tier4/caret_analyze/issues/117>`_ (`#122 <https://github.com/tier4/caret_analyze/issues/122>`_)
* fix(plot): missing column name with node granularity (`#119 <https://github.com/tier4/caret_analyze/issues/119>`_)
* fix(lttng): unsupported operand type error with cache. (`#118 <https://github.com/tier4/caret_analyze/issues/118>`_)
  * rm: unused variables
  * fix: unsupported operand type error
* fix(lttng): error on LttngEventFilter caused by `#100 <https://github.com/tier4/caret_analyze/issues/100>`_ (`#117 <https://github.com/tier4/caret_analyze/issues/117>`_)
* perf(lttng): improve memory consumption (`#100 <https://github.com/tier4/caret_analyze/issues/100>`_)
  * remove ros2_tracing impl
  * fix: test errors caused by forced_conversion option
  * Update src/test/infra/lttng/test_architecture_reader_lttng.py
  Co-authored-by: Takayuki AKAMINE <38586589+takam5f2@users.noreply.github.com>
  * fix: cannot picke SwigPyObject in yaml dump
  * refactor(lttng): wrap bt2 to EventCollection
  * feat: support lttng cache
  * add: tqdm for converting and loading
  * fix: giving events as arguments
  * fix: flake8
  * add: force_conversion option
  Co-authored-by: Takayuki AKAMINE <38586589+takam5f2@users.noreply.github.com>
* feat: add API to plot topic_info (`#108 <https://github.com/tier4/caret_analyze/issues/108>`_)
  * feat: add template for API to plot topic_info
  * feat: add comm_XXX_plot.to_dataframe() method
  * feat: add comm_XXX_plot.show() method
  * fix(plot): fix argument passing
  * feat(plot): pubsub period and pubsub frequency
  * refactor(plot): eliminate duplicate implementations
  * fix(plot): interactive drawing of all topics
  * fix: pass flake8
  * feat(application): add get_pub_subs()
  * fix(plot): improve display
  * refactor(plot): improve readability
  * fix(plot): add remove_dropped into to_dataframe
  * fix(plot): separate ColorSelector
  * fix(application): fix get_pub_subs to use get_communications
  * fix(application): separate get_pub_subs
  * fix(plot): skip processing if latency_table size is 0
  * fix: pass flake8
  * fix: typo
* fix(lttng_info): trying to merge on object and float64 columns (`#115 <https://github.com/tier4/caret_analyze/issues/115>`_)
* docs(record_cpp): add warnings section for records.data (`#114 <https://github.com/tier4/caret_analyze/issues/114>`_)
  * docs(record_cpp): add warnings section for records.data
  * add: see also section
* perf(response_time): improve ResponseMap creation time (`#112 <https://github.com/tier4/caret_analyze/issues/112>`_)
* docs(response_time): modify example (`#111 <https://github.com/tier4/caret_analyze/issues/111>`_)
* docs(path_base): modify docstrings with return value (`#110 <https://github.com/tier4/caret_analyze/issues/110>`_)
* chore: change plot APIs to return figure object (`#109 <https://github.com/tier4/caret_analyze/issues/109>`_)
* feat(plot): add Path object into target in callback_info and callback_sched (`#106 <https://github.com/tier4/caret_analyze/issues/106>`_)
  * feat: add PathBase into CallbackTypes in callback_info
  * feat(plot): add Path and CallbackBase into CallbackTypes
  * feat: add Application, Path, and List[CallbackGroup] into target of callback_sched
  * refoctor: move UniqueList class into common module
  * fix(plot): change set to UniqueList to keep order
* chore: rename 'jitter' to 'period' in Plot (`#107 <https://github.com/tier4/caret_analyze/issues/107>`_)
  * chore: rename 'jitter' to 'period' in Plot
  * fix: pass flake8 test
  * fix: leave create_callback_jitter_plot
* refactor: wrap column name string to column value class (`#99 <https://github.com/tier4/caret_analyze/issues/99>`_)
  * refactor: wrap column name string to column value
  * modify response records to use ColumnValue
* refactor(records): support multiple argument dispatching to append (`#98 <https://github.com/tier4/caret_analyze/issues/98>`_)
  * refactor(record): support dict type arguments in append
  * add: python3-multimethod-pip as depend
  * typo
  * fix: record base not found error in github actions
* feat: response time records (`#96 <https://github.com/tier4/caret_analyze/issues/96>`_)
  * add: response_time.py
  * add: response historam module
  * pass flake8
  * add docstring
  * add: tests for to_response_records
  * done small todos
  * add response time to __init_\_.py
  * add usage to docstring. and fix typo
  * typo
  * support density argument
  * modify histogram definition
  * correct comment
  * update docstring for ReponseRecords
  * modify columns args to optional
  * modify min_time to measurement start time
  * remove duplicative line
  * add: to_timeseries api
  * Change the order of class definitions
  * add: to_histogram for best or worst case
  * modify docstrings
  * refactor: response histogram class
  * refactor: test_response_time.py
  * typo
  * fix: flake8
  * fix: change input_min_time to first valid message
  * fix: error in to_histogram
  * remove unnecessary lines
  * remove unnecessary for loop
  * clean response map
* feat: display legends by 10 and add full_legends option in plot.show() (`#95 <https://github.com/tier4/caret_analyze/issues/95>`_)
  * feat(plot): legends display by 10 & add full_legends option
  * refactor(plot): improve readability
  * feat(plot): add warning message
  * docs: fix full_legend -> full_legends in docstring
  * refactor(plot): remove unnecessary dict
  * style(plot): add comment of the reason for the value of num_legend_threshold
* fix: time stamp style for message flow (`#97 <https://github.com/tier4/caret_analyze/issues/97>`_)
* docs: add docstring for Callback Scheduling Visualization (`#90 <https://github.com/tier4/caret_analyze/issues/90>`_)
  * docs: add docstring for Callback Scheduling Visualization
  * fix: Correct mistakes in documents
* test(records_cpp): enable ModuleNotFoundError in local environment (`#94 <https://github.com/tier4/caret_analyze/issues/94>`_)
  * test(records_cpp): enable ModuleNotFoundError in local environment
  * add description for module not found erro
* fix(records_cpp): wrong invalid argument error (`#93 <https://github.com/tier4/caret_analyze/issues/93>`_)
* chore(records_cpp): change export_json to export_yaml (`#92 <https://github.com/tier4/caret_analyze/issues/92>`_)
  * mod(records_cpp): change export_json to export_yaml
  * add: python3-yaml as exec-depend
* chore: move files in tmp to a correct dir (`#89 <https://github.com/tier4/caret_analyze/issues/89>`_)
* refactor(data_model): sort tid and pid arguments. (`#87 <https://github.com/tier4/caret_analyze/issues/87>`_)
  * add: test_latency_definitions.py
  * fix: _build_nodes_df keyerr
  * add: subscription_callback property for NodePathValue
  * refactor(data_model): sort add_timer arguments
  * sort add_context argument
  * sort add_node arguments
* test(lttng): add latency definition test (`#86 <https://github.com/tier4/caret_analyze/issues/86>`_)
  * add: test_latency_definitions.py
  * fix: _build_nodes_df keyerr
  * add: subscription_callback property for NodePathValue
* fix: add docstring to find similar one function (`#85 <https://github.com/tier4/caret_analyze/issues/85>`_)
  * fix: add docstring to find similar one function
  * fix: adapt flake8
  Co-authored-by: Keita Miura <miura2445@mail.saitama-u.ac.jp>
* Contributors: Bo Peng, atsushi yano, hsgwa, keita1523, takeshi-iwanari

0.2.3 (2022-07-11)
------------------
* docs(runtime): add docstrings (`#74 <https://github.com/tier4/caret_analyze/issues/74>`_)
  * add: docstrings for runtime packages
  * pass: pep257
  * fix: typo
* fix: fix 'int' to 'datetime' in Lttng.get_trace_range (`#82 <https://github.com/tier4/caret_analyze/issues/82>`_)
  * fix: 'int' -> 'datetime' in Lttng.get_trace_range
  * fix: typo
* feat: get measure duration and trace creation datetime (`#80 <https://github.com/tier4/caret_analyze/issues/80>`_)
  * feat: get measure duration and trace creation datetime
  * docs: add discription
  * docs: add underbar to docstring
  * refactor: get_trace_datetime & get_trace_range
  * fix: review comment
  * fix: pass flake8
* feat: add regular expression function to get_callbacks(). (`#36 <https://github.com/tier4/caret_analyze/issues/36>`_)
  * feat: add regular expression functionality
  to get_callbacks().
  * fix: delete  lines
  * fix: modify wrong message.
  * test: add get_callbacks() unit test
  * fix: add error handling to get_callbacks()
  * fix: add test_getcallbacks()
  * fix: delete line
  * fix: add line
  * fix: modify test code
  * chore: add requirements.txt and Levenshtein
  * fix: modify function name
  * fix: add '-y' in pytest.yaml
  * fix: sample constraction
  * fix: delete requirements.txt because already existing
  * feat: adapt find_similar_one function (except get_communication and get_node_paths)
  * fix: easy miss
  * fix: add threshold
  * fix: change import package from levenshtein to difflib because license
  * feat: add find_similar_one_multi_keys (prototype)
  * feat: add find_similar_one_multi_keys (prototype)
  * feat: adapt find_similar_one or find_similar_one_multi_keys to all get\_** function
  * fix: delete comment out and add a requirement package
  * feat: test code for find_similar
  * fix: add util test and adapt flake8
  * fix: delete blank line
  * fix: modify error message
  * fix: delete levenshtein
  * fix: adapt flake8
  * fix: add test code giving the most similar values
  * fix: add return in get_node_paths
  * fix: delete statistics in requiremtns.txt
  * fix: modify threshold from 0.8 to 0.6
  * fix: adapt flake8
  Co-authored-by: keita1523 <keita.miura@tier4.jp>
* feat: automatically adjust Bokeh plot width to the screen size of device (`#78 <https://github.com/tier4/caret_analyze/issues/78>`_)
* fix: fix the problem of can only disapper a part graph (`#76 <https://github.com/tier4/caret_analyze/issues/76>`_)
  * fix: automatically adjust Bokeh plot size to the screen size of device used and fix only can only disappear a part of disabled graph
  * fix: fix the problem of can only disapper a part graph
  Fixing the problem of can only disappear a part of the graph of disabled legend.
  But still can`t disappeared the arrow in the graph.
  * fix: fix spell error
  * Revert "fix: fix spell error"
  This reverts commit af052f39aca7a5b870f0fcbf5094017a6d67eb53.
  * fix: fix spell error
  * docs: add comment
* feat: clarify the reason for the warning in CallbackChain.verify (`#77 <https://github.com/tier4/caret_analyze/issues/77>`_)
  * style: pass flake8
  * feat: clarify the reason for the warning in path.verify
  * test: add test_verify of CallbackChain
  * fix: add half-width space
  * fix: add half-width space in test
* feat: check callback uniqueness (`#72 <https://github.com/tier4/caret_analyze/issues/72>`_)
  * add: pytest.ini to test logger message
  * implement uniqueness check
  * add: blank line
  * modify warning message
  * pass tests
  * typo
  Co-authored-by: hsgwa <hasegawa.isp@gmail.com>
* fix: lttng and architecture call order dependency. disable lttng singleton (`#73 <https://github.com/tier4/caret_analyze/issues/73>`_)
  * fix: Lttng and Architecture call order dependency. disable lttng singleton.
  * pass test when caret_analyze_cpp_impl is disabled.
  * fix: add -y flag to apt-get install
  Co-authored-by: hsgwa <hasegawa.isp@gmail.com>
* fix: treat drop as delay bugs in plot and to_hist (`#70 <https://github.com/tier4/caret_analyze/issues/70>`_)
  * fix: treat_drop_as_delay in chain_latency plot
  * fix treat_drop_as_delay in to_hist
  * fix: treat_drop_as_delay in chain_latency plot for granularity == 'node'
  Co-authored-by: hsgwa <hasegawa.isp@gmail.com>
* docs: add pages of API for developer and fix document generation methods (`#71 <https://github.com/tier4/caret_analyze/issues/71>`_)
  * docs: add API for developer
  * docs: add PathBase to all
  * docs: remove TODO
  * docs: correction to lowercase
  * docs: correction to lowercase in .pages
  * docs: move architecture and infra to API for user
* feat(Plot): add arguments for export graphs as file (`#65 <https://github.com/tier4/caret_analyze/issues/65>`_)
  * feat(Plot): add arguments for export graphs as file
  * fix(Plot): reorder packages to suppress warnings
  * remove unncessary line.
* docs: add docstring for API to get information per callback (`#67 <https://github.com/tier4/caret_analyze/issues/67>`_)
  * docs: add docstring for callback_info
  * docs: pass test
  * docs: add notes of to_dataframe
  * docs: pass flake8
  * docs: add TimeSeriesPlot to __all\_\_
  * docs: pass flake8
  * docs: add CallbackXXXPlot classes to __all\_\_
  * docs: fix typo
  * docs: pass flake8
  * docs: fix 'simtime' -> 'sim_time'
* chore: skip mypy tests in github actions (`#69 <https://github.com/tier4/caret_analyze/issues/69>`_)
  Co-authored-by: hsgwa <hasegawa.isp@gmail.com>
* chore: pass tests except mypy (`#68 <https://github.com/tier4/caret_analyze/issues/68>`_)
  * rm: test_record_factory
  * mod: pass flake8
  * pass copyright
  * fix: skip cpp records tests when cppimpl is disabled
  Co-authored-by: hsgwa <hasegawa.isp@gmail.com>
* feat: show and hide plots (`#62 <https://github.com/tier4/caret_analyze/issues/62>`_)
* chore: change repository structure (`#57 <https://github.com/tier4/caret_analyze/issues/57>`_)
  * change repository structure
  * chore: reduce structure layer with removing caret_analyze directory
* Contributors: Bo Peng, Takayuki AKAMINE, atsushi yano, hsgwa, kuboichiTakahisa

0.2.2 (2022-04-27)
------------------

0.2.1 (2022-01-17)
------------------
