-- A solution contains projects, and defines the available configurations
solution "gist"
  configurations { "Debug", "Release" }
 
-- Avoid error when invoking genie --help.
if (_ACTION == nil) then return end
location ("../build/" .. _ACTION) 
libdirs ("../build/" .. _ACTION)
language "C++"
includedirs "../include"
flags { "ExtraWarnings", "FatalWarnings" }
startproject "cmdline"

-- A project defines one build target
project "gist"
  kind "StaticLib"
  files { "../include/**.h", "../src/**.cpp", "*.lua" }
  targetdir ("../build/" .. _ACTION)

project "cmdline"
  kind "ConsoleApp"
  files { "../tests/cmdline/**.cpp" }
  links { "gist" }
  targetdir "../bin"

