#include "test.hpp"
#include <lowfive/H5VOLProperty.hpp>
#include <lowfive/vol-dist-metadata.hpp>

int main(int argc, char** argv)
{
    return test::run(argc, argv, [](const std::string& name)
    {
        using Dist = LowFive::DistMetadataVOL;
        LowFive::VOLBase* original = nullptr;
        if (name == "base-metadata" || name == "base-distributed")
            original = &LowFive::VOLBase::create_vol_base();
        else if (name == "metadata-repeat" || name == "metadata-distributed")
            original = &LowFive::MetadataVOL::create_MetadataVOL();
        else if (name == "distributed-repeat" || name == "distributed-metadata")
        {
            auto& vol = Dist::create_DistMetadataVOL(MPI_COMM_WORLD, Dist::communicators{});
            vol.serve_on_close = false;
            original = &vol;
        }
        else CHECK(false);
        LowFive::H5VOLProperty property(*original);
        const bool incompatible = name == "base-metadata" || name == "base-distributed" ||
                                  name == "metadata-distributed";
        bool rejected = false;
        try
        {
            LowFive::VOLBase* requested = nullptr;
            if (name == "base-metadata" || name == "metadata-repeat" ||
                name == "distributed-metadata")
                requested = &LowFive::MetadataVOL::create_MetadataVOL();
            else if (name == "base-distributed")
                requested = &Dist::create_DistMetadataVOL(MPI_COMM_WORLD, MPI_COMM_NULL);
            else
                requested = &Dist::create_DistMetadataVOL(MPI_COMM_WORLD, Dist::communicators{});
            CHECK(requested == original);
        }
        catch (const std::logic_error&) { rejected = true; }
        CHECK(rejected == incompatible);
        CHECK(LowFive::VOLBase::info->vol == original);
        CHECK(&LowFive::VOLBase::create_vol_base() == original);

        const std::string filename = "factory-" + name + ".h5";
        {
            test::Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
            property.apply(fapl);
            test::Id file(H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl), H5Fclose);
            test::Id group(H5Gcreate2(file, "still-usable", H5P_DEFAULT,
                                     H5P_DEFAULT, H5P_DEFAULT), H5Gclose);
            CHECK(H5Lexists(file, "still-usable", H5P_DEFAULT) > 0);
        }
        std::remove(filename.c_str());
    });
}
