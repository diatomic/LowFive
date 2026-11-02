#include "test.hpp"
#include <diy/master.hpp>
#include <diy/decomposition.hpp>
#include "../examples/prod-con-multidata/prod-con-multidata.hpp"

int main(int argc, char** argv)
{
    return test::run(argc, argv, [](const std::string& name)
    {
        if (name == "serialization")
        {
            Bounds core(3), bounds(3), domain(3);
            for (int j = 0; j < 3; ++j)
            {
                core.min[j] = j + 2;
                core.max[j] = j + 4;
                bounds.min[j] = j + 1;
                bounds.max[j] = j + 5;
                domain.min[j] = 0;
                domain.max[j] = 9;
            }
            Block source(core, bounds, domain);
            diy::MemoryBuffer buffer;
            Block::save(&source, buffer);
            buffer.reset();
            std::unique_ptr<Block> restored(static_cast<Block*>(Block::create()));
            Block::load(restored.get(), buffer);
            for (int j = 0; j < 3; ++j)
            {
                CHECK(restored->core.min[j] == core.min[j]);
                CHECK(restored->core.max[j] == core.max[j]);
            }
            return;
        }

        const bool particles = name.find("particles") == 0;
        const bool valid = name.find("valid") != std::string::npos && name.find("invalid") == std::string::npos;
        Bounds domain(3);
        for (int j = 0; j < 3; ++j) { domain.min[j] = 0; domain.max[j] = 1; }
        diy::mpi::communicator world;
        diy::Master master(world, 1, -1, Block::create, Block::destroy);
        auto* block = new Block(domain, domain, domain);
        master.add(1, block, new diy::Link);
        block->generate_grid();
        block->generate_sequenced_points(1, 4);
        // Native HDF5 core driver: deterministic fixture with no shared disk filename.
        test::Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
        CHECK(H5Pset_fapl_core(fapl, 4096, false) >= 0);
        test::Id file(H5Fcreate("example-validation.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl), H5Fclose);
        hsize_t dims[] = {8, 3};
        test::Id space(particles ? H5Screate_simple(2, dims, nullptr) :
                                  H5Screate_simple(3, std::vector<hsize_t>{2,2,2}.data(), nullptr), H5Sclose);
        test::Id dataset(H5Dcreate2(file, "data", H5T_NATIVE_FLOAT, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
        std::vector<float> values(particles ? 24 : 8);
        for (size_t i = 0; i < values.size(); ++i) values[i] = particles ? i / 3 : i;
        if (name == "grid-corrupt") values[3] = -1;
        if (name == "particles-duplicate") values[5 * 3] = 4;
        if (name == "particles-reordered")
            for (size_t j = 0; j < 3; ++j) std::swap(values[4 * 3 + j], values[5 * 3 + j]);
        if (name == "particles-coordinate") values[6 * 3 + 2] = 7;
        CHECK(H5Dwrite(dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, values.data()) >= 0);
        hid_t read_id = name.find("read-error") != std::string::npos ? H5I_INVALID_HID : dataset.id;
        bool rejected = false;
        try
        {
            auto cp = master.proxy(0);
            if (particles) block->read_block_points(cp, read_id, 8, 2);
            else block->read_block_grid(cp, read_id);
        }
        catch (const std::exception&) { rejected = true; }
        CHECK(rejected != valid);
    });
}
