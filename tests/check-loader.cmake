execute_process(COMMAND ${command}
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error
    TIMEOUT 15)

# The MPI launcher must report ordinary failure, not a signal or timeout.
if(NOT result STREQUAL "1")
    message(FATAL_ERROR "Expected loader exit 1, got '${result}'\n${output}${error}")
endif()
set(diagnostics "${output}${error}")
foreach(expected IN ITEMS "${expected_path}" "${expected_symbol}")
    string(FIND "${diagnostics}" "${expected}" position)
    if(position EQUAL -1)
        message(FATAL_ERROR "Missing loader diagnostic '${expected}'\n${diagnostics}")
    endif()
endforeach()
