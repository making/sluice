package am.ik.sluice.server;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

@AnalyzeClasses(packages = "am.ik.sluice.server", importOptions = ImportOption.DoNotIncludeTests.class)
class PackageStructureArchTest {

	@ArchTest
	static final ArchRule packagesAreFreeOfCycles = slices().matching("am.ik.sluice.server.(*)..")
		.should()
		.beFreeOfCycles();

}
