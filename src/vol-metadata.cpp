#include <lowfive/vol-metadata.hpp>
#include "metadata.hpp"
#include "vol-metadata-private.hpp"

#include <unordered_set>

namespace
{
LowFive::Object* detached_owner(LowFive::File& file,
                                const std::unordered_set<LowFive::Object*>& detached,
                                LowFive::Object* object)
{
    if (!object || object->find_root() != &file) return nullptr;
    while (object->parent) object = object->parent;
    return detached.count(object) ? object : nullptr;
}

void collect_unlinked(LowFive::MetadataVOL& vol, LowFive::File& file)
{
    std::unordered_set<LowFive::Object*> detached(file.unlinked.begin(),
                                                  file.unlinked.end());
    std::unordered_set<LowFive::Object*> marked;
    std::vector<LowFive::Object*> pending;
    auto mark = [&](LowFive::Object* target)
    {
        auto* owner = detached_owner(file, detached, target);
        if (owner && marked.insert(owner).second)
            pending.push_back(owner);
    };
    auto scan_links = [&](LowFive::Object* object, auto&& scan_links_ref) -> void
    {
        if (auto* link = dynamic_cast<LowFive::HardLink*>(object))
            mark(link->target);
        for (auto* child : object->children)
            scan_links_ref(child, scan_links_ref);
    };

    for (auto* pointer : vol.our_objects)
    {
        auto* object = static_cast<LowFive::Object*>(
            static_cast<LowFive::ObjectPointers*>(pointer)->mdata_obj);
        mark(object);
    }
    for (const auto& entry : vol.files)
        if (auto* source = dynamic_cast<LowFive::File*>(entry.second))
            for (const auto& reference : source->references)
            {
                const auto& state = reference.second;
                const bool targets_file =
                    state.filename.empty() ? source == &file :
                    state.filename == file.name;
                if (targets_file) mark(file.find_token(state.token));
            }

    scan_links(&file, scan_links);
    for (size_t i = 0; i < pending.size(); ++i)
        scan_links(pending[i], scan_links);

    for (auto it = file.unlinked.begin(); it != file.unlinked.end();)
    {
        if (marked.count(*it))
            ++it;
        else
        {
            auto* object = *it;
            it = file.unlinked.erase(it);
            delete object;
        }
    }
}

template<class Visitor>
void visit_stored_references(LowFive::Object* object, Visitor&& visit)
{
    if (auto* attribute = dynamic_cast<LowFive::Attribute*>(object))
    {
        if (attribute->data)
            attribute->mem_type.visit_references(
                attribute->space.size(), attribute->data.get(), visit);
        return;
    }
    auto* dataset = dynamic_cast<LowFive::Dataset*>(object);
    if (!dataset) return;
    if (dataset->fill_data)
        dataset->type.visit_references(1, dataset->fill_data.get(), visit);
    for (auto& triple : dataset->data)
        if (triple.owned_data)
            triple.type.visit_references(
                triple.memory.size(), triple.owned_data.get(), visit);
}

void suspend_pending_references(LowFive::File& file)
{
    for (auto& pending : file.references)
        if (pending.second.resolved)
        {
            if (H5Rdestroy(pending.first) < 0)
                throw LowFive::MetadataError(
                    "Could not suspend HDF5 reference during file cleanup");
            std::memset(pending.first, 0, sizeof(*pending.first));
            pending.second.resolved = false;
        }
}

bool has_wrapper(const LowFive::MetadataVOL& vol, LowFive::Object* root,
                 const LowFive::ObjectPointers* ignored = nullptr)
{
    for (auto* pointer : vol.our_objects)
    {
        auto* wrapped = static_cast<LowFive::ObjectPointers*>(pointer);
        auto* file = dynamic_cast<LowFive::File*>(root);
        const bool reference_only =
            wrapped->reference_file_wrapper && file &&
            file->reference_file_id >= 0 &&
            H5Iget_ref(file->reference_file_id) <= 1;
        if (wrapped != ignored && !reference_only && wrapped->mdata_obj &&
            static_cast<LowFive::Object*>(wrapped->mdata_obj)->find_root() ==
                root)
            return true;
    }
    return false;
}

std::unordered_set<LowFive::Object*> reachable_files(
    const LowFive::MetadataVOL& vol,
    const LowFive::ObjectPointers* ignored = nullptr)
{
    std::unordered_set<LowFive::Object*> live;
    std::vector<LowFive::Object*> pending;
    auto mark = [&](LowFive::Object* root)
    {
        if (root && live.insert(root).second)
            pending.push_back(root);
    };

    for (const auto& entry : vol.files)
    {
        auto* root = entry.second;
        auto* file = dynamic_cast<LowFive::File*>(root);
        if (has_wrapper(vol, root, ignored) ||
            (vol.keep && file && !file->remote))
            mark(root);
    }
    for (size_t i = 0; i < pending.size(); ++i)
    {
        auto* source = dynamic_cast<LowFive::File*>(pending[i]);
        if (!source) continue;
        for (const auto& reference : source->references)
        {
            auto target = vol.files.find(reference.second.filename);
            if (target != vol.files.end())
                mark(target->second);
        }
    }
    return live;
}

LowFive::Object* reference_target(
    LowFive::File& file, const LowFive::File::PendingReference& state)
{
    auto* target = file.find_token(state.token);
    return target && !file.reachable_path(target).empty() ? target : nullptr;
}

LowFive::File* reference_file(
    LowFive::MetadataVOL& vol, LowFive::File* source,
    const LowFive::File::PendingReference& state)
{
    if (state.filename.empty() ||
        (source && state.filename == source->name)) return source;
    auto found = vol.files.find(state.filename);
    return found == vol.files.end() ?
        nullptr : dynamic_cast<LowFive::File*>(found->second);
}

bool is_lowfive_file(hid_t id)
{
    hid_t connector = H5VLget_connector_id(id);
    hid_t lowfive = H5VLget_connector_id_by_value(
        static_cast<H5VL_class_value_t>(510));
    int comparison = -1;
    const bool result =
        connector >= 0 && lowfive >= 0 &&
        H5VLcmp_connector_cls(&comparison, connector, lowfive) >= 0 &&
        comparison == 0;
    if (connector >= 0) H5VLclose(connector);
    if (lowfive >= 0) H5VLclose(lowfive);
    return result;
}

herr_t close_reference_file(LowFive::File& file)
{
    if (file.reference_file_id < 0) return 0;
    const hid_t id =
        std::exchange(file.reference_file_id, H5I_INVALID_HID);
    return H5Fclose(id);
}


void retarget_links(LowFive::Object* object, LowFive::Object* previous,
                    LowFive::Object* replacement)
{
    if (auto* link = dynamic_cast<LowFive::HardLink*>(object))
        if (link->target == previous) link->target = replacement;
    for (auto* child : object->children)
        retarget_links(child, previous, replacement);
}

LowFive::Object* replace_metadata_impl(
    LowFive::MetadataVOL& vol, LowFive::File& file,
    LowFive::Object* previous, LowFive::Object* replacement)
{
    auto* parent = previous->parent;
    if (parent)
    {
        auto position = std::find(parent->children.begin(),
                                  parent->children.end(), previous);
        if (position == parent->children.end())
        {
            delete replacement;
            return nullptr;
        }
        *position = replacement;
    }
    else
    {
        auto position =
            std::find(file.unlinked.begin(), file.unlinked.end(), previous);
        if (position == file.unlinked.end())
        {
            delete replacement;
            return nullptr;
        }
        *position = replacement;
    }

    replacement->name = previous->name;
    replacement->extra = std::exchange(previous->extra, nullptr);
    replacement->token = previous->token;
    replacement->connector_token = previous->connector_token;
    replacement->has_connector_token = previous->has_connector_token;
    replacement->parent = parent;
    replacement->set_root(&file);
    replacement->move_children(previous);

    retarget_links(&file, previous, replacement);
    for (auto* detached : file.unlinked)
        retarget_links(detached, previous, replacement);
    for (auto* pointer : vol.our_objects)
    {
        auto* wrapped = static_cast<LowFive::ObjectPointers*>(pointer);
        if (wrapped->mdata_obj == previous)
            wrapped->mdata_obj = replacement;
    }
    previous->parent = nullptr;
    delete previous;
    return replacement;
}

LowFive::Object* cache_reference_target(
    LowFive::MetadataVOL& vol, LowFive::File* source,
    const LowFive::File::PendingReference& state, hid_t target_id)
{
    auto* file = reference_file(vol, source, state);
    if (!file) return nullptr;
    auto* existing = file->find_token(state.token);
    if (existing && file->reachable_path(existing).empty())
        existing = nullptr;
    if (!existing && !state.path.empty())
    {
        auto located = file->search(state.path);
        if (located.path.empty()) existing = located.obj;
    }
    const auto type = H5Iget_type(target_id);
    const bool concrete =
        (type == H5I_DATASET && dynamic_cast<LowFive::Dataset*>(existing)) ||
        (type == H5I_GROUP &&
         (existing == file || dynamic_cast<LowFive::Group*>(existing))) ||
        (type == H5I_DATATYPE &&
         dynamic_cast<LowFive::CommittedDatatype*>(existing));
    if (concrete)
    {
        existing->token = state.token;
        existing->connector_token = state.connector_token;
        existing->has_connector_token = true;
        return existing;
    }
    if (state.path.empty()) return nullptr;

    LowFive::Object* target = nullptr;
    if (type == H5I_DATASET)
    {
        LowFive::Hid datatype(H5Dget_type(target_id), true);
        if (datatype.id < 0) return nullptr;
        LowFive::Hid dataspace(H5Dget_space(target_id), true);
        if (dataspace.id < 0) return nullptr;
        LowFive::Hid dcpl(H5Dget_create_plist(target_id), true);
        if (dcpl.id < 0) return nullptr;
        LowFive::Hid dapl(H5Dget_access_plist(target_id), true);
        if (dapl.id < 0) return nullptr;
        target = new LowFive::Dataset(
            "", datatype.id, dataspace.id, LowFive::Dataset::Ownership::lowfive,
            dcpl, dapl, true, false);
    }
    else if (type == H5I_GROUP)
    {
        LowFive::Hid gcpl(H5Gget_create_plist(target_id), true);
        if (gcpl.id < 0) return nullptr;
        target = new LowFive::Group("", gcpl.id);
    }
    else if (type == H5I_DATATYPE)
    {
        auto* datatype = new LowFive::CommittedDatatype("", target_id);
        if (datatype->data.empty())
        {
            delete datatype;
            return nullptr;
        }
        target = datatype;
    }
    else
        return nullptr;

    target = existing ?
        replace_metadata_impl(vol, *file, existing, target) :
        file->add_path(target, state.path);
    if (!target) return nullptr;
    target->token = state.token;
    target->connector_token = state.connector_token;
    target->has_connector_token = true;
    return target;
}

void collect_files(LowFive::MetadataVOL& vol)
{
    if (vol.collecting_files) return;
    vol.collecting_files = true;
    struct Reset
    {
        bool& value;
        ~Reset() { value = false; }
    } reset {vol.collecting_files};

    const auto live = reachable_files(vol);
    for (const auto& entry : vol.files)
        if (!live.count(entry.second))
            if (auto* file = dynamic_cast<LowFive::File*>(entry.second))
                suspend_pending_references(*file);

    for (const auto& entry : vol.files)
        if (!live.count(entry.second))
            if (auto* file = dynamic_cast<LowFive::File*>(entry.second))
                if (close_reference_file(*file) < 0)
                    throw LowFive::MetadataError(
                        "Could not close retained reference file");

    for (auto it = vol.files.begin(); it != vol.files.end();)
    {
        if (live.count(it->second))
            ++it;
        else
        {
            auto* root = it->second;
            it = vol.files.erase(it);
            delete root;
        }
    }
}

}
LowFive::Object*
LowFive::replace_metadata(MetadataVOL& vol, File& file, Object* previous,
                          Object* replacement)
{
    return replace_metadata_impl(vol, file, previous, replacement);
}


LowFive::Object::ObjectPath
LowFive::metadata_target_path(Object* parent, const std::string& path)
{
    unsigned budget = 64;
    Object* current = !path.empty() && path.front() == '/' ?
        parent->find_root() : parent;
    std::string remaining = path;
    while (true)
    {
        size_t offset = 0;
        while (offset < remaining.size() && remaining[offset] == '/') ++offset;
        if (offset == remaining.size()) return {current, ""};
        const auto end = remaining.find('/', offset);
        const auto component = remaining.substr(
            offset, end == std::string::npos ? end : end - offset);
        const auto suffix = end == std::string::npos ?
            std::string {} : remaining.substr(end + 1);
        if (component == ".")
        {
            remaining = suffix;
            continue;
        }

        Object* child = nullptr;
        for (auto* candidate : current->children)
            if (candidate->type != ObjectType::Attribute &&
                candidate->name == component)
            {
                child = candidate;
                break;
            }
        if (!child) return {current, remaining.substr(offset)};
        if (auto* link = dynamic_cast<SoftLink*>(child))
        {
            if (!budget--) return {nullptr, ""};
            remaining = link->target;
            if (!suffix.empty())
                remaining += (remaining.empty() || remaining.back() == '/' ?
                              "" : "/") + suffix;
            current = !remaining.empty() && remaining.front() == '/' ?
                current->find_root() : link->parent;
            continue;
        }
        current = child->resolve_link(budget);
        if (!current) return {nullptr, remaining.substr(offset)};
        remaining = suffix;
    }
}

herr_t
LowFive::MetadataVOL::
retain_reference_file(File& file, hid_t target_id)
{
    if (file.reference_file_id >= 0) return 0;

    hid_t candidate = H5I_INVALID_HID;
    ObjectPointers* retained_wrapper = nullptr;
    const bool lowfive_target = is_lowfive_file(target_id);
    if (lowfive_target)
    {
        retaining_reference_file = true;
        candidate = H5Iget_file_id(target_id);
        retaining_reference_file = false;
        if (candidate < 0) return -1;

        for (auto* pointer : our_objects)
        {
            auto* wrapped = static_cast<ObjectPointers*>(pointer);
            if (wrapped->reference_file_wrapper && wrapped->mdata_obj &&
                static_cast<Object*>(wrapped->mdata_obj)->find_root() == &file)
            {
                retained_wrapper = wrapped;
                break;
            }
        }
        if (!retained_wrapper)
        {
            void* candidate_object = H5VLobject(candidate);
            for (auto* pointer : our_objects)
            {
                auto* wrapped = static_cast<ObjectPointers*>(pointer);
                if (candidate_object && wrapped->file_wrapper &&
                    wrapped->mdata_obj &&
                    static_cast<Object*>(wrapped->mdata_obj)->find_root() ==
                        &file &&
                    unwrap(wrapped) &&
                    VOLBase::wrap_get_object(unwrap(wrapped)) ==
                        candidate_object)
                {
                    retained_wrapper = wrapped;
                    break;
                }
            }
        }
        if (!retained_wrapper)
            for (auto* pointer : our_objects)
            {
                auto* wrapped = static_cast<ObjectPointers*>(pointer);
                if (wrapped->primary_file_wrapper && wrapped->mdata_obj &&
                    static_cast<Object*>(wrapped->mdata_obj)->find_root() ==
                        &file)
                {
                    retained_wrapper = wrapped;
                    break;
                }
            }
    }
    else
    {
        Hid target_file(H5Iget_file_id(target_id), true);
        unsigned long target_number = 0;
        const ssize_t count =
            H5Fget_obj_count(H5F_OBJ_ALL, H5F_OBJ_FILE);
        if (target_file.id < 0 || count < 0 ||
            H5Fget_fileno(target_file.id, &target_number) < 0)
            return -1;
        std::vector<hid_t> candidates(static_cast<size_t>(count));
        const ssize_t found = H5Fget_obj_ids(
            H5F_OBJ_ALL, H5F_OBJ_FILE, candidates.size(),
            candidates.data());
        if (found < 0) return -1;
        for (ssize_t i = 0; i < found; ++i)
        {
            unsigned long number = 0;
            if (is_lowfive_file(candidates[static_cast<size_t>(i)]) &&
                H5Fget_fileno(candidates[static_cast<size_t>(i)], &number) >=
                    0 &&
                number == target_number)
            {
                candidate = candidates[static_cast<size_t>(i)];
                break;
            }
        }
        if (candidate < 0) return -1;

        void* target_object = H5VLobject(candidate);
        for (auto* pointer : our_objects)
        {
            auto* wrapped = static_cast<ObjectPointers*>(pointer);
            if (wrapped->file_wrapper && wrapped->mdata_obj &&
                static_cast<Object*>(wrapped->mdata_obj)->find_root() == &file &&
                unwrap(wrapped) &&
                VOLBase::wrap_get_object(unwrap(wrapped)) == target_object)
            {
                retained_wrapper = wrapped;
                break;
            }
        }
        if (retained_wrapper && H5Iinc_ref(candidate) < 0)
            retained_wrapper = nullptr;
    }
    if (!retained_wrapper)
    {
        if (lowfive_target && candidate >= 0) H5Fclose(candidate);
        return -1;
    }

    file.reference_file_id = candidate;
    retained_wrapper->reference_file_wrapper = true;
    return 0;
}


LowFive::ObjectPointers*
LowFive::MetadataVOL::
wrap(void* p)
{
    ObjectPointers* op = new ObjectPointers;
    op->h5_obj = p;

    our_objects.insert(op);

    return op;
}

void*
LowFive::MetadataVOL::
unwrap(void* p)
{
    ObjectPointers* op = static_cast<ObjectPointers*>(p);
    return op ? op->h5_obj : nullptr;
}


void
LowFive::
unlink_metadata(MetadataVOL& vol, Object* object)
{
    if (!object) return;
    auto* file = dynamic_cast<File*>(object->find_root());
    if (!file)
    {
        bool referenced = false;
        for (auto* pointer : vol.our_objects)
        {
            auto* current = static_cast<Object*>(
                static_cast<ObjectPointers*>(pointer)->mdata_obj);
            while (current && current != object)
                current = current->parent;
            if (current)
            {
                referenced = true;
                break;
            }
        }
        object->remove();
        if (!referenced) delete object;
        return;
    }
    object->remove();
    file->unlinked.push_back(object);
    collect_unlinked(vol, *file);
}

void
LowFive::MetadataVOL::
drop(void* p)
{
    if (!p) return;
    ObjectPointers* op = static_cast<ObjectPointers*>(p);
    Object* root = op->mdata_obj ?
        static_cast<Object*>(op->mdata_obj)->find_root() : nullptr;
    our_objects.erase(p);
    delete op;
    if (!root) return;
    if (auto* file = dynamic_cast<File*>(root))
        collect_unlinked(*this, *file);
    if (has_wrapper(*this, root)) return;
    auto entry = files.find(root->name);
    if (entry != files.end() && entry->second == root)
        collect_files(*this);
    else
        delete root;
}

LowFive::MetadataVOL::
~MetadataVOL()
{
    auto log = get_logger();
    log->trace("MetadataVOL dtor, closing retained reference files");
    collecting_files = true;
    for (const auto& entry : files)
        if (auto* file = dynamic_cast<File*>(entry.second))
            close_reference_file(*file);
    release_wrapped_objects();
    log->trace("MetadataVOL dtor, calling clear_files");
    clear_files();
    log->trace("MetadataVOL dtor, clear files done");
}

void
LowFive::MetadataVOL::
drop(std::string filename)
{
    auto it = files.find(filename);
    if (it == files.end()) return;
    Object* root = it->second;
    if (auto* file = dynamic_cast<File*>(root))
    {
        const bool was_collecting = collecting_files;
        collecting_files = true;
        close_reference_file(*file);
        collecting_files = was_collecting;
    }
    files.erase(it);
    for (auto* pointer : our_objects)
    {
        auto* other = static_cast<ObjectPointers*>(pointer);
        if (other->mdata_obj && static_cast<Object*>(other->mdata_obj)->find_root() == root) return;
    }
    delete root;
}

void
LowFive::MetadataVOL::
print_files()
{
    for (auto& f : files)
    {
        fmt::print("--------------------------------------------------------------------------------\n");
        fmt::print("File {}\n", f.first);
        f.second->print(0);
        fmt::print("--------------------------------------------------------------------------------\n");
    }
}

// locate an object in the metadata of one file by its full path, which uniquely identifies one object
LowFive::Object*
LowFive::MetadataVOL::
locate(std::string filename, std::string full_path) const
{
    auto it = files.find(filename);
    if (it == files.end())
        return NULL;

    return it->second->search(full_path).exact();
}


void
LowFive::MetadataVOL::
clear_files()
{
    while (!files.empty()) drop(files.begin()->first);
}

LowFive::MetadataVOL& LowFive::MetadataVOL::
create_MetadataVOL()
{
    auto log = get_logger();
    log->trace("Enter get_metadata_vol");

    if (!info->vol)
    {
        log->trace("In get_metadata_vol: info->vol is NULL, creating new MetadataVOL");
        info->vol = new MetadataVOL();
    } else
    {
        log->warn("In get_metadata_vol: info->vol is not NULL, return existing object");
    }

    return *dynamic_cast<MetadataVOL*>(info->vol);
}

LowFive::File::PendingReference
LowFive::describe_reference(
    H5R_ref_t& reference, MetadataVOL* vol, File* source)
{
    File::PendingReference result;
    result.type = H5Rget_type(&reference);
    if (result.type == H5R_BADTYPE)
        throw MetadataError("Cannot describe an invalid HDF5 reference");

    auto name = [&](auto getter)
    {
        const auto length = getter(&reference, nullptr, 0);
        if (length < 0)
            throw MetadataError("Could not inspect HDF5 reference");
        const auto size = static_cast<size_t>(length);
        std::string value(size + 1, '\0');
        if (getter(&reference, value.data(), value.size()) < 0)
            throw MetadataError("Could not inspect HDF5 reference");
        value.resize(size);
        return value;
    };
    result.filename = name(H5Rget_file_name);
    const auto path_length =
        H5Rget_obj_name(&reference, H5P_DEFAULT, nullptr, 0);
    if (path_length < 0)
        throw MetadataError("Could not inspect HDF5 reference path");
    result.path.resize(static_cast<size_t>(path_length) + 1);
    if (H5Rget_obj_name(&reference, H5P_DEFAULT, result.path.data(),
                        result.path.size()) < 0)
        throw MetadataError("Could not inspect HDF5 reference path");
    result.path.resize(static_cast<size_t>(path_length));

    Hid target(H5Ropen_object(&reference, H5P_DEFAULT, H5P_DEFAULT), true);
    H5O_info2_t info {};
    if (target.id < 0 ||
        H5Oget_info(target.id, &info, H5O_INFO_BASIC) < 0)
        throw MetadataError("Could not inspect HDF5 reference target");
    std::memcpy(&result.token, info.token.__data, sizeof(result.token));
    result.connector_token = info.token;
    if (vol)
    {
        cache_reference_target(*vol, source, result, target.id);
        if (auto* file = reference_file(*vol, source, result))
            if (vol->retain_reference_file(*file, target.id) < 0)
                throw MetadataError("Could not retain HDF5 reference file");
    }

    if (result.type == H5R_DATASET_REGION1 ||
        result.type == H5R_DATASET_REGION2)
    {
        hid_t region = H5Ropen_region(&reference, H5P_DEFAULT, H5P_DEFAULT);
        if (region < 0)
            throw MetadataError("Could not inspect HDF5 region reference");
        result.region = Dataspace(region, true);
    }
    else if (result.type == H5R_ATTR)
        result.attribute = name(H5Rget_attr_name);

    return result;
}

void
LowFive::MetadataVOL::
suspend_references(Object* o)
{
    auto* file = dynamic_cast<File*>(o->find_root());
    assert(file);

    visit_stored_references(o, [&](H5R_ref_t* reference)
    {
        auto pending = file->references.find(reference);
        if (pending != file->references.end())
        {
            if (pending->second.resolved)
            {
                if (H5Rdestroy(reference) < 0)
                    throw MetadataError("Could not suspend HDF5 reference");
                std::memset(reference, 0, sizeof(*reference));
                pending->second.resolved = false;
            }
            return;
        }
        const H5R_ref_t empty {};
        if (std::memcmp(reference, &empty, sizeof(empty)) == 0) return;
        if (H5Rget_type(reference) == H5R_BADTYPE) return;

        auto state = describe_reference(*reference, this, file);
        auto* target_file = reference_file(*this, file, state);
        if (!target_file) return;
        auto* cached = reference_target(*target_file, state);
        if (!cached || dynamic_cast<DummyObject*>(cached))
        {
            H5Rdestroy(reference);
            std::memset(reference, 0, sizeof(*reference));
            throw MetadataError("Could not cache HDF5 reference target");
        }
        auto inserted = file->references.emplace(reference, std::move(state));
        if (H5Rdestroy(reference) < 0)
        {
            file->references.erase(inserted.first);
            throw MetadataError("Could not suspend HDF5 reference");
        }
        std::memset(reference, 0, sizeof(*reference));
    });
}

void
LowFive::MetadataVOL::
resolve_reference(File& file, H5R_ref_t* reference)
{
    auto pending = file.references.find(reference);
    if (pending == file.references.end()) return;
    if (pending->second.resolved) return;
    const auto state = pending->second;
    auto* target_file = reference_file(*this, &file, state);
    if (!target_file)
        throw MetadataError("Reference target file was not found");

    auto create = [&](hid_t id, const char* path)
    {
        if (state.type == H5R_OBJECT1 || state.type == H5R_OBJECT2)
            return H5Rcreate_object(id, path, H5P_DEFAULT, reference);
        if (state.type == H5R_DATASET_REGION1 ||
            state.type == H5R_DATASET_REGION2)
            return H5Rcreate_region(id, path, state.region.id,
                                    H5P_DEFAULT, reference);
        if (state.type == H5R_ATTR)
            return H5Rcreate_attr(id, path, state.attribute.c_str(),
                                 H5P_DEFAULT, reference);
        return herr_t {-1};
    };

    Object* target = reference_target(*target_file, state);
    if (!target || dynamic_cast<DummyObject*>(target))
        throw MetadataError("Reference target was not found");
    target->token = state.token;
    target->connector_token = state.connector_token;
    target->has_connector_token = true;

    herr_t created = -1;
    if (target_file->reference_file_id >= 0)
    {
        Hid opened(H5Oopen_by_token(target_file->reference_file_id,
                                    state.connector_token), true);
        if (opened.id >= 0)
            created = create(opened.id, ".");
    }
    if (created < 0)
    {
        ObjectPointers* pointers = wrap(nullptr);
        pointers->mdata_obj = target;
        pointers->tmp = true;
        hid_t id = H5VLwrap_register(pointers, get_identifier_type(target));
        if (id < 0)
        {
            drop(pointers);
            throw MetadataError("Could not register reference target");
        }
        created = create(id, ".");
        if (H5Idec_ref(id) < 0)
            throw MetadataError("Could not close reference target");
    }
    if (created < 0)
        throw MetadataError("Could not create HDF5 reference");
    pending->second.resolved = true;
}

void
LowFive::MetadataVOL::
resolve_references(Object* object)
{
    auto* file = dynamic_cast<File*>(object->find_root());
    assert(file);
    if (file->references.empty()) return;
    try
    {
        visit_stored_references(
            object,
            [&](H5R_ref_t* reference)
            {
                resolve_reference(*file, reference);
            });
    }
    catch (...)
    {
        suspend_references(object);
        throw;
    }
}

void
LowFive::MetadataVOL::
resolve_references(Dataset* dataset, hid_t memory_space, hid_t file_space)
{
    auto* file = dynamic_cast<File*>(dataset->find_root());
    assert(file);
    if (file->references.empty()) return;
    try
    {
        if (dataset->fill_data)
            dataset->type.visit_references(
                1, dataset->fill_data.get(),
                [&](H5R_ref_t* reference)
                {
                    resolve_reference(*file, reference);
                });
        dataset->visit_read_references(
            memory_space, file_space,
            [&](H5R_ref_t* reference)
            {
                resolve_reference(*file, reference);
            });
    }
    catch (...)
    {
        suspend_references(dataset);
        throw;
    }
}
